"""
Models 3 & 4: Macro Statistical Modeling 
========================================

Model 3: Weather-Price Cross-Correlation
Model 4: Price Elasticity estimation

This script runs a monthly batch process that looks back over recent history 
to calculate two types of strategic insights for every product/pincode pair:
1. Does extreme weather correlate with price hikes?
2. What is the implied price elasticity based on local competitor pricing and discount depth?

Outputs are written to Postgres analytical tables: `ml_weather_correlations` and `ml_price_elasticity`.
"""

import logging
import pandas as pd
import numpy as np
import scipy.stats as stats
from sqlalchemy import create_engine
import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("ml.statistical")

# PostgreSQL connection string
DB_USER = "postgres"
DB_PASS = "user"
DB_HOST = "127.0.0.1"
DB_PORT = "5433"
DB_NAME = "marketintel"
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class StructuralModelTrainer:
    def __init__(self, tracking_uri: str = "sqlite:///mlflow.db"):
        self.engine = create_engine(DATABASE_URI)
        mlflow.set_tracking_uri(tracking_uri)
        self._init_tables()

    def _init_tables(self):
        """Creates the strategic analytical output tables if they don't exist."""
        ddl_weather = """
        CREATE TABLE IF NOT EXISTS ml_weather_correlations (
            product_id VARCHAR(64),
            pincode VARCHAR(10),
            pearson_r DECIMAL(5,4),
            p_value DECIMAL(5,4),
            temp_sensitivity VARCHAR(20),
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id, pincode)
        );
        """
        ddl_elasticity = """
        CREATE TABLE IF NOT EXISTS ml_price_elasticity (
            product_id VARCHAR(64),
            pincode VARCHAR(10),
            elasticity_coefficient DECIMAL(10,4),
            discount_sensitivity VARCHAR(20),
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (product_id, pincode)
        );
        """
        with self.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(text(ddl_weather))
            conn.execute(text(ddl_elasticity))

    def fetch_weather_and_price_history(self, days=30) -> pd.DataFrame:
        """Joins daily product pricing with spatial daily weather facts."""
        logger.info(f"Fetching {days} days of pricing + weather joined history...")
        
        query = f"""
            SELECT 
                p.product_id,
                p.pincode,
                DATE(p.scraped_at) as ds,
                AVG(p.selling_price) as price,
                AVG(w.temp_max_c) as temp,
                AVG(w.precipitation_mm) as rainfall
            FROM fact_pricing_snapshots p
            JOIN fact_daily_weather w ON p.pincode = w.pincode AND DATE(p.scraped_at) = w.target_date
            WHERE p.scraped_at >= NOW() - INTERVAL '{days} days'
            AND p.selling_price IS NOT NULL
            GROUP BY p.product_id, p.pincode, DATE(p.scraped_at)
        """
        df = pd.read_sql(query, self.engine)
        logger.info(f"Fetched {len(df)} joined weather/price records.")
        return df

    def calculate_correlations(self, df_joined: pd.DataFrame):
        """Model 3: Pearson Correlation for Weather vs Price."""
        mlflow.set_experiment("Weather_Price_Correlation")
        
        results = []
        groups = df_joined.groupby(['product_id', 'pincode'])
        logger.info(f"Calculating weather correlations for {len(groups)} series...")
        
        # Keep track of significant correlations
        sig_count = 0 
        
        with mlflow.start_run(run_name="monthly_correlation_scan"):
            for (pid, pin), ts in groups:
                if len(ts) < 5 or ts['price'].nunique() == 1:
                    continue # Not enough variance to correlate
                
                # We calculate Pearson correlation between Temp and Price
                # In agricultural products, higher temp/rainfall might spike prices.
                r, p = stats.pearsonr(ts['temp'].fillna(0), ts['price'])
                
                sensitivity = "Neutral"
                if p < 0.05: # Statistically significant
                    if r > 0.5:
                        sensitivity = "Highly Positive"
                    elif r > 0.2:
                        sensitivity = "Positive"
                    elif r < -0.5:
                        sensitivity = "Highly Negative"
                    elif r < -0.2:
                        sensitivity = "Negative"
                    
                    if abs(r) > 0.2:
                        sig_count += 1
                        
                results.append({
                    "product_id": pid,
                    "pincode": pin,
                    "pearson_r": round(float(r) if not np.isnan(r) else 0.0, 4),
                    "p_value": round(float(p) if not np.isnan(p) else 1.0, 4),
                    "temp_sensitivity": sensitivity
                })
        
            mlflow.log_metric("total_series_analyzed", len(groups))
            mlflow.log_metric("weather_sensitive_products", sig_count)
            
        if results:
            df_res = pd.DataFrame(results)
            # Upsert logic via temp table or delete-insert
            with self.engine.begin() as conn:
                 from sqlalchemy import text
                 conn.execute(text("TRUNCATE TABLE ml_weather_correlations"))
            
            df_res.to_sql('ml_weather_correlations', self.engine, if_exists='append', index=False)
            logger.info(f"Committed {len(df_res)} weather correlation metrics.")


    def fetch_elasticity_history(self, days=30) -> pd.DataFrame:
        """Fetch discount percentages vs stockout ratios to proxy price elasticity."""
        logger.info("Fetching discount vs availability history...")
        query = f"""
            SELECT 
                product_id,
                pincode,
                DATE(scraped_at) as ds,
                AVG(discount_pct) as avg_discount,
                AVG(CASE WHEN in_stock THEN 1.0 ELSE 0.0 END) as stock_ratio
            FROM fact_pricing_snapshots
            WHERE scraped_at >= NOW() - INTERVAL '{days} days'
            GROUP BY product_id, pincode, DATE(scraped_at)
        """
        return pd.read_sql(query, self.engine)

    def calculate_elasticity(self, df: pd.DataFrame):
        """Model 4: Price Elasticity Proxy."""
        mlflow.set_experiment("Price_Elasticity")
        
        results = []
        groups = df.groupby(['product_id', 'pincode'])
        
        logger.info(f"Calculating elasticity proxies for {len(groups)} series...")
        with mlflow.start_run(run_name="monthly_elasticity_scan"):
            for (pid, pin), ts in groups:
                 if len(ts) < 5 or ts['avg_discount'].nunique() == 1:
                     continue
                     
                 # Basic proxy: correlation between discount depth and stock-outs.
                 # If discounting heavily leads to 0 stock_ratio, demand is highly elastic.
                 # negative correlation = high elasticity (discount goes up, stock goes down)
                 r, p = stats.pearsonr(ts['avg_discount'].fillna(0), ts['stock_ratio'].fillna(1))
                 
                 label = "Inelastic"
                 if p < 0.1 and r < -0.3:
                     label = "Elastic"
                 elif p < 0.1 and r < -0.6:
                     label = "Highly Elastic"
                     
                 results.append({
                    "product_id": pid,
                    "pincode": pin,
                    "elasticity_coefficient": round(float(r) if not np.isnan(r) else 0.0, 4),
                    "discount_sensitivity": label
                 })
                 
            mlflow.log_metric("elasticity_analyzed", len(groups))

        if results:
            df_res = pd.DataFrame(results)
            with self.engine.begin() as conn:
                 from sqlalchemy import text
                 conn.execute(text("TRUNCATE TABLE ml_price_elasticity"))
            
            df_res.to_sql('ml_price_elasticity', self.engine, if_exists='append', index=False)
            logger.info(f"Committed {len(df_res)} elasticity metrics.")

if __name__ == "__main__":
    trainer = StructuralModelTrainer()
    
    # 1. Weather Correlations
    weather_df = trainer.fetch_weather_and_price_history(days=45)
    if not weather_df.empty:
        trainer.calculate_correlations(weather_df)
    else:
        logger.warning("No joined weather/pricing data found.")
        
    # 2. Elasticity Proxies
    elasticity_df = trainer.fetch_elasticity_history(days=45)
    if not elasticity_df.empty:
        trainer.calculate_elasticity(elasticity_df)
    else:
        logger.warning("No elasticity data found.")
