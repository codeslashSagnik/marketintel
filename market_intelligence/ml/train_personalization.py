"""
Models 6 & 7: User Personalization and Ranking
==============================================

Model 6: Deterministic Value Scoring
Model 7: Personalized Ranking Matrix

This script runs daily to compute a universal "Value Score" for every product,
combining the current price discount with the Model 1 Forecast. 
Then, it matches these scores against simulated User Profiles (Model 6) to
generate the Top 10 recommendations per user.

Outputs:
- `ml_value_scores`: Daily computed product value scores
- `ml_user_profiles`: Exponentially decayed category affinities per user
- `ml_recommendations`: Final Top 10 lists ready for LLM narration
"""

import sys
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("ml.personalization")

# PostgreSQL connection string
DB_USER = "postgres"
DB_PASS = "user"
DB_HOST = "127.0.0.1"
DB_PORT = "5433"
DB_NAME = "marketintel"
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

class RankerAndProfiler:
    def __init__(self, tracking_uri: str = "sqlite:///mlflow.db"):
        self.engine = create_engine(DATABASE_URI)
        mlflow.set_tracking_uri(tracking_uri)
        self._init_tables()

    def _init_tables(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS ml_value_scores (
            product_id VARCHAR(64),
            pincode VARCHAR(10),
            value_score DECIMAL(5,2),
            price_drop_weight DECIMAL(5,2),
            forecast_weight DECIMAL(5,2),
            calculated_at DATE DEFAULT CURRENT_DATE,
            PRIMARY KEY (product_id, pincode, calculated_at)
        );
        
        CREATE TABLE IF NOT EXISTS ml_user_profiles (
            user_id VARCHAR(50),
            pincode VARCHAR(10),
            category_l2 VARCHAR(200),
            affinity_score DECIMAL(5,4),
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (user_id, category_l2)
        );
        
        CREATE TABLE IF NOT EXISTS ml_recommendations (
            user_id VARCHAR(50),
            product_id VARCHAR(64),
            pincode VARCHAR(10),
            rank_position INT,
            final_match_score DECIMAL(5,2),
            generated_at DATE DEFAULT CURRENT_DATE,
            PRIMARY KEY (user_id, product_id, generated_at)
        );
        """
        with self.engine.begin() as conn:
            from sqlalchemy import text
            conn.execute(text(ddl))

    def compute_value_scores(self):
        """Model 6: Deterministic Value Scoring."""
        logger.info("Computing universal Value Scores (Discount + 7-Day Forecast Match)...")
        mlflow.set_experiment("Value_Scoring")
        
        # 1. Fetch Today's latest prices and join with the 7-day forecast
        # We calculate:
        # Score = (Discount_Pct * 0.4) + (CurrentPrice / ForecastPrice * 0.6)
        # If ForecastPrice is higher than Current, buy now! Ratio > 1.
        
        query = """
            SELECT 
                p.product_id,
                p.pincode,
                p.selling_price,
                p.discount_pct,
                f.predicted_price as forecast_price_7d
            FROM v_latest_prices p
            LEFT JOIN ml_forecasts f 
                ON p.product_id = f.product_id 
                AND p.pincode = f.pincode
                AND f.target_date = CURRENT_DATE + INTERVAL '7 days'
            WHERE p.selling_price > 0
        """
        df = pd.read_sql(query, self.engine)
        
        with mlflow.start_run(run_name=f"daily_value_scores_{datetime.now().strftime('%Y%m%d')}"):
        
            # Fill missing forecasts (assume flat price if no forecast exists)
            df['forecast_price_7d'] = df['forecast_price_7d'].fillna(df['selling_price'])
            
            # Sub-scores (Base 100)
            df['discount_score'] = df['discount_pct'].clip(0, 100)
            
            # Forecast Score: (Forecast - Current) / Current -> % increase expected. 
            # If price is expected to rise 20%, you should buy now (Score +20)
            df['expected_increase_pct'] = ((df['forecast_price_7d'] - df['selling_price']) / df['selling_price']) * 100
            df['forecast_score'] = df['expected_increase_pct'].clip(0, 50) * 2 # Bound and scale
            
            # Weighted Final Value Score
            df['value_score'] = (df['discount_score'] * 0.5) + (df['forecast_score'] * 0.5)
            
            # Prepare to write
            df_out = df[['product_id', 'pincode', 'value_score', 'discount_score', 'forecast_score']].copy()
            df_out.columns = ['product_id', 'pincode', 'value_score', 'price_drop_weight', 'forecast_weight']
            df_out['calculated_at'] = datetime.now().date()
            
            mlflow.log_metric("total_scored_products", len(df_out))
            mlflow.log_metric("avg_value_score", df_out['value_score'].mean())
            
            with self.engine.begin() as conn:
                 from sqlalchemy import text
                 # Clean today's run if rerunning
                 conn.execute(text("DELETE FROM ml_value_scores WHERE calculated_at = CURRENT_DATE"))
                 
            df_out.to_sql('ml_value_scores', self.engine, if_exists='append', index=False)
            logger.info(f"Committed {len(df_out)} Value Scores to database.")
            
        return df_out

    def generate_simulated_user_profiles(self, n_users=5):
        """Model 6/7 Helper: For demonstration, we simulate 5 users with distinct category affinities."""
        logger.info(f"Simulating {n_users} user interest profiles...")
        
        # In production this queries user click/cart history and applies exponential decay
        profiles = [
            {"user_id": "usr_fitness_freak", "pincode": "700020", "category_l2": "Fitness Supplements", "affinity_score": 0.95},
            {"user_id": "usr_fitness_freak", "pincode": "700020", "category_l2": "Eggs, Meat & Fish", "affinity_score": 0.88},
            
            {"user_id": "usr_baker", "pincode": "700020", "category_l2": "Bakery, Cakes & Dairy", "affinity_score": 0.92},
            {"user_id": "usr_baker", "pincode": "700020", "category_l2": "Foodgrains, Oil & Masala", "affinity_score": 0.75},
            
            {"user_id": "usr_snacker", "pincode": "700020", "category_l2": "Snacks & Branded Foods", "affinity_score": 0.98},
            {"user_id": "usr_snacker", "pincode": "700020", "category_l2": "Beverages", "affinity_score": 0.80},
        ]
        
        df_prof = pd.DataFrame(profiles)
        
        with self.engine.begin() as conn:
             from sqlalchemy import text
             conn.execute(text("TRUNCATE TABLE ml_user_profiles"))
             
        df_prof.to_sql('ml_user_profiles', self.engine, if_exists='append', index=False)
        return df_prof
        
    def personalize_rankings(self):
         """Model 7: Match Profiles against Value Scores to generate Top 10 lists."""
         logger.info("Generating Top 10 Personalized Rankings (Model 7)...")
         mlflow.set_experiment("Personalized_Ranking")
         
         # 1. Fetch Today's Value Scores joined with Product Meta
         # 2. Cross Join with User Profiles on Category L2
         query = """
             WITH product_values AS (
                 SELECT 
                     v.product_id, v.pincode, v.value_score,
                     p.category_l2
                 FROM ml_value_scores v
                 JOIN dim_product p ON v.product_id = p.product_id
                 WHERE v.calculated_at = CURRENT_DATE
             ),
             user_matrix AS (
                 SELECT 
                     u.user_id, u.pincode, u.category_l2, u.affinity_score
                 FROM ml_user_profiles u
             )
             SELECT 
                 u.user_id,
                 p.product_id,
                 u.pincode,
                 p.value_score,
                 u.affinity_score,
                 ((u.affinity_score * 0.7 * 100) + (p.value_score * 0.3)) as final_match_score
             FROM user_matrix u
             JOIN product_values p 
                  ON u.category_l2 = p.category_l2 
                  AND u.pincode = p.pincode
         """
         
         df_ranked = pd.read_sql(query, self.engine)
         
         with mlflow.start_run(run_name=f"daily_ranking_{datetime.now().strftime('%Y%m%d')}"):
            
             if df_ranked.empty:
                 logger.warning("No matches found between user profiles and value scores. Check categories/pincodes.")
                 return
                 
             # Sort and Keep Top 10 Per User
             df_ranked = df_ranked.sort_values(['user_id', 'final_match_score'], ascending=[True, False])
             top10 = df_ranked.groupby('user_id').head(10).copy()
             
             # Add exact rank position
             top10['rank_position'] = top10.groupby('user_id').cumcount() + 1
             top10['generated_at'] = datetime.now().date()
             
             # Prep for DB
             out = top10[['user_id', 'product_id', 'pincode', 'rank_position', 'final_match_score', 'generated_at']]
             
             with self.engine.begin() as conn:
                  from sqlalchemy import text
                  conn.execute(text("DELETE FROM ml_recommendations WHERE generated_at = CURRENT_DATE"))
                  
             out.to_sql('ml_recommendations', self.engine, if_exists='append', index=False)
             
             logger.info(f"Committed {len(out)} Personalized Rankings for {top10['user_id'].nunique()} users.")
             mlflow.log_metric("total_users_ranked", top10['user_id'].nunique())
             mlflow.log_metric("avg_top1_score", out[out['rank_position']==1]['final_match_score'].mean())

if __name__ == "__main__":
    job = RankerAndProfiler()
    # 1. Deterministic Value Score (Model 6)
    job.compute_value_scores()
    
    # 2. Simulate User Histories (Model 6 Part B)
    job.generate_simulated_user_profiles()
    
    # 3. Match and Rank (Model 7)
    job.personalize_rankings()
