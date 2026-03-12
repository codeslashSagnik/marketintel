"""
Model 2: Anomaly Detection (Training specific)
==============================================

This script trains an Isolation Forest model on historical data
to identify multidimensional point anomalies, and prepares a CUSUM
configuration for trend detection.

It registers the trained artifact into MLflow.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
import mlflow
import mlflow.sklearn

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
logger = logging.getLogger("ml.anomaly_detector")

# PostgreSQL connection string
DB_USER = "postgres"
DB_PASS = "user"
DB_HOST = "127.0.0.1"
DB_PORT = "5433"
DB_NAME = "marketintel"
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class AnomalyDetectorTrainer:
    def __init__(self, tracking_uri: str = "sqlite:///mlflow.db"):
        self.engine = create_engine(DATABASE_URI)
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("Anomaly_Detection")

    def fetch_training_data(self, days=30) -> pd.DataFrame:
        """Fetch the last 30 days of pricing snapshots for training."""
        logger.info(f"Fetching {days} days of historical data from PostgreSQL...")
        
        # We query the v_latest_prices view or raw facts. Since fact_pricing_snapshots
        # is a timeseries, we'll query it directly.
        query = f"""
            SELECT 
                f.product_id,
                f.pincode,
                f.selling_price,
                f.mrp,
                f.discount_pct,
                f.price_change_pct,
                f.in_stock,
                EXTRACT(DOW FROM f.scraped_at) as day_of_week
            FROM fact_pricing_snapshots f
            WHERE f.scraped_at >= NOW() - INTERVAL '{days} days'
            AND f.selling_price IS NOT NULL
            AND f.selling_price > 0
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Fetched {len(df)} records for training.")
        return df

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare features for Isolation Forest."""
        # Fill missing values
        df['discount_pct'] = df['discount_pct'].fillna(0)
        df['price_change_pct'] = df['price_change_pct'].fillna(0)
        df['in_stock'] = df['in_stock'].astype(int) # True/False -> 1/0
        
        # We normalize prices relative to MRP to make the model product-agnostic
        df['price_to_mrp_ratio'] = np.where(df['mrp'] > 0, df['selling_price'] / df['mrp'], 1.0)
        
        return df.dropna(subset=['selling_price'])

    def train_model(self, df: pd.DataFrame, contamination=0.01):
        """Train standard Isolation Forest to detect point anomalies."""
        features = ['price_to_mrp_ratio', 'discount_pct', 'price_change_pct', 'in_stock', 'day_of_week']
        
        X = df[features]
        if len(X) < 100:
            logger.warning("Not enough data to train Isolation Forest securely. Need >100 rows.")
            # We train anyway for demonstration
            if len(X) == 0:
                logger.error("0 rows found. Cannot train.")
                return None

        logger.info(f"Training Isolation Forest with contamination={contamination} on {len(X)} samples...")
        
        model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X)
        
        # Evaluate internally on the train set (find out how many it flags)
        df['anomaly_score'] = model.decision_function(X) # lower is more anomalous
        df['is_anomaly'] = model.predict(X) == -1
        
        anom_frac = df['is_anomaly'].sum() / len(df)
        logger.info(f"Model trained. It flagged {anom_frac*100:.2f}% of the training set as anomalies.")

        return model

    def run_pipeline(self):
        try:
            with mlflow.start_run(run_name="weekly_iso_forest_training"):
                df_raw = self.fetch_training_data()
                
                if df_raw.empty:
                    logger.warning("No data returned from DB. Exiting pipeline.")
                    return
                
                df_feat = self.engineer_features(df_raw)
                
                # We expect 1% anomalies naturally in retail data
                contamination = 0.01
                model = self.train_model(df_feat, contamination)
                
                if model:
                    mlflow.log_param("contamination", contamination)
                    mlflow.log_param("n_estimators", 100)
                    mlflow.log_metric("training_samples", len(df_feat))
                    
                    # Log the sklearn model directly to MLflow!
                    mlflow.sklearn.log_model(model, "isolation_forest_model")
                    logger.info("Successfully pushed Isolation Forest to MLflow tracking server.")
                    
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    trainer = AnomalyDetectorTrainer()
    trainer.run_pipeline()
