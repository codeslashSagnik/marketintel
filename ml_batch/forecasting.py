import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from prophet import Prophet
import mlflow
from sqlalchemy import create_engine

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_forecasting")

# ── Configuration ──────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB   = os.environ.get("PG_DB", "market_intel")
PG_USER = os.environ.get("PG_USER", "mi_admin")
PG_PASS = os.environ.get("PG_PASS", "market_intel_2026")

DB_URI = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("market_intel_price_forecasting")

def load_historical_data(engine, days_back=90) -> pd.DataFrame:
    """Load daily median prices per product and pincode."""
    query = f"""
        SELECT 
            product_id, 
            pincode, 
            DATE(scraped_at) as ds, 
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY selling_price) as y
        FROM fact_pricing_snapshots
        WHERE quality_flag != 'rejected'
          AND scraped_at >= NOW() - INTERVAL '{days_back} days'
        GROUP BY product_id, pincode, DATE(scraped_at)
        HAVING COUNT(*) >= 5  -- Only products with sufficient history
    """
    logger.info(f"Loading up to {days_back} days of historical data...")
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])
    return df

def retrain_models(engine, df: pd.DataFrame):
    """Weekly Retraining loop: trains a Prophet model per (product_id, pincode) group."""
    logger.info("Starting Weekly Retraining Loop...")
    
    groups = df.groupby(['product_id', 'pincode'])
    
    with mlflow.start_run(run_name=f"weekly_retraining_{datetime.now().strftime('%Y%m%d')}") as parent_run:
        mlflow.log_metric("total_series_trained", len(groups))
        
        for (prod_id, pin), group in groups:
            if len(group) < 14:
                # Need at least 14 points for a decent forecast
                continue
                
            with mlflow.start_run(run_name=f"model_{prod_id[:8]}_{pin}", nested=True):
                # Prophet expects columns 'ds' and 'y'
                train_df = group[['ds', 'y']].sort_values('ds')
                
                # Setup model
                model = Prophet(
                    yearly_seasonality=False,
                    weekly_seasonality=True,
                    daily_seasonality=False,
                    changepoint_prior_scale=0.05
                )
                
                model.fit(train_df)
                
                # Cross validation metrics would go here. For now, tracking basic metrics.
                mlflow.log_params({
                    "product_id": prod_id,
                    "pincode": pin,
                    "training_points": len(train_df)
                })
                
                # Log model. Model name formatting to be safe for MLflow registry
                model_name = f"prophet_{prod_id}_{pin}"
                mlflow.prophet.log_model(model, artifact_path="model", registered_model_name=model_name)
                
                # Auto-transition to production
                client = mlflow.tracking.MlflowClient()
                latest_versions = client.get_latest_versions(name=model_name, stages=["None"])
                if latest_versions:
                    v = latest_versions[0].version
                    client.transition_model_version_stage(
                        name=model_name,
                        version=v,
                        stage="Production",
                        archive_existing_versions=True
                    )
        
    logger.info("Retraining complete.")


def run_inference(engine, df: pd.DataFrame):
    """Daily Batch Inference: Loads Production models and predicts next 7 days."""
    logger.info("Starting Daily Batch Inference...")
    groups = df.groupby(['product_id', 'pincode'])
    
    client = mlflow.tracking.MlflowClient()
    predictions_to_insert = []
    today = pd.to_datetime(datetime.now().date())
    
    for (prod_id, pin), group in groups:
        model_name = f"prophet_{prod_id}_{pin}"
        try:
            # Try loading the production model
            model_uri = f"models:/{model_name}/Production"
            model = mlflow.prophet.load_model(model_uri)
        except Exception as e:
            logger.debug(f"No production model found for {prod_id}-{pin}: {e}")
            continue
            
        # Predict next 7 days starting from tomorrow
        future = model.make_future_dataframe(periods=7, freq='D')
        # Only keep the future rows
        future = future[future['ds'] > today]
        
        if future.empty:
            continue
            
        forecast = model.predict(future)
        
        # Prepare the row for ml_predictions
        pred_dict = {
            "product_id": prod_id,
            "pincode": pin,
            "model_type": "prophet",
            "model_version": "production",
            "predicted_at": datetime.now()
        }
        
        # Map values to predicted_price_d1 ... d7
        for idx, row in enumerate(forecast.itertuples()):
            if idx < 7:
                pred_dict[f"predicted_price_d{idx+1}"] = row.yhat
                
        # Average CI for simplicity
        pred_dict["ci_lower"] = forecast['yhat_lower'].mean()
        pred_dict["ci_upper"] = forecast['yhat_upper'].mean()
        
        predictions_to_insert.append(pred_dict)

    if predictions_to_insert:
        preds_df = pd.DataFrame(predictions_to_insert)
        preds_df.to_sql('ml_predictions', engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(predictions_to_insert)} daily forecasts into PostgreSQL.")
    else:
        logger.warning("No predictions generated. Have models been trained?")

def main():
    parser = argparse.ArgumentParser(description="Model 1: Price Forecasting")
    parser.add_argument("--mode", choices=["train", "predict"], required=True, help="Run mode: train (weekly) or predict (daily)")
    args = parser.parse_args()

    engine = create_engine(DB_URI)
    
    try:
        df = load_historical_data(engine, days_back=90)
        
        if df.empty:
            logger.warning("Not enough historical data found to run models.")
            return

        if args.mode == "train":
            retrain_models(engine, df)
        elif args.mode == "predict":
            run_inference(engine, df)
            
    except Exception as e:
        logger.error(f"Forecasting job failed: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
