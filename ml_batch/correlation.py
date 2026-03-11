import os
import argparse
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_correlation")

# ── Configuration ──────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB   = os.environ.get("PG_DB", "market_intel")
PG_USER = os.environ.get("PG_USER", "mi_admin")
PG_PASS = os.environ.get("PG_PASS", "market_intel_2026")

DB_URI = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Maximum lag (in days) to search for optimal correlation between Weather and Price
MAX_LAGS = 5 

def compute_monthly_correlations(engine):
    """Monthly batch job: computes cross-correlation between weather and aggregated prices for each category/pincode mapping"""
    logger.info("Starting Monthly Weather-Price Correlation Analysis...")
    
    # 1. Fetch daily averages per category and pincode for last 60 days
    # (Since weather affects categories mostly, not specific items)
    query_prices = """
        SELECT 
            p.category_l1 as product_category,
            f.pincode,
            DATE(f.scraped_at) as ds,
            AVG(f.selling_price) as avg_price
        FROM fact_pricing_snapshots f
        JOIN dim_product p ON f.product_id = p.product_id
        WHERE f.quality_flag != 'rejected' 
          AND f.scraped_at >= NOW() - INTERVAL '60 days'
        GROUP BY p.category_l1, f.pincode, DATE(f.scraped_at)
        HAVING p.category_l1 IS NOT NULL
    """
    
    query_weather = """
        SELECT 
            pincode,
            target_date as ds,
            temp_max_c,
            precipitation_mm
        FROM fact_daily_weather
        WHERE target_date >= CURRENT_DATE - INTERVAL '60 days'
    """
    
    logger.info("Loading Price Aggregations...")
    df_prices = pd.read_sql(query_prices, engine)
    df_prices['ds'] = pd.to_datetime(df_prices['ds'])

    logger.info("Loading Weather Histories...")
    df_weather = pd.read_sql(query_weather, engine)
    df_weather['ds'] = pd.to_datetime(df_weather['ds'])
    
    if df_prices.empty or df_weather.empty:
        logger.warning("Insufficient data (Price or Weather) to compute correlations. Exiting.")
        return

    # Merge prices and weather on pincode and date
    df_merged = pd.merge(df_prices, df_weather, on=['pincode', 'ds'], how='inner')
    
    if df_merged.empty:
        logger.warning("No overlapping dates between weather and pricing facts. Exiting.")
        return

    # 2. Compute Pearson Correlation per (Category, Pincode) with time-shifts (Lags)
    groups = df_merged.groupby(['product_category', 'pincode'])
    
    results = []
    
    from scipy.stats import pearsonr
    
    for (category, pin), group in groups:
        if len(group) < 14:  # Need at least two weeks of overlap
            continue
            
        group = group.sort_values('ds')
        
        # Test diffs/pct_change for stationarity? 
        # For a basic baseline, we just use absolute values and smooth them
        group['price_smooth'] = group['avg_price'].rolling(window=3, min_periods=1).mean()
        group['temp_smooth'] = group['temp_max_c'].rolling(window=3, min_periods=1).mean()
        group['rain_smooth'] = group['precipitation_mm'].rolling(window=3, min_periods=1).mean()
        
        # We will track 'temp_max_c' and 'precipitation_mm'
        for weather_col in ['temp_smooth', 'rain_smooth']:
            best_r = 0
            best_lag = 0
            best_p = 1.0
            
            # Cross-correlation essentially: shift the weather BACKWARDS (meaning yesterday's weather affects today's price)
            for lag in range(0, MAX_LAGS + 1):
                lagged_weather = group[weather_col].shift(lag)
                
                # Drop NAs
                valid_mask = ~lagged_weather.isna() & ~group['price_smooth'].isna()
                x = lagged_weather[valid_mask]
                y = group['price_smooth'][valid_mask]
                
                if len(x) < 10:
                    continue
                    
                r, p_val = pearsonr(x, y)
                
                # Maximize absolute correlation
                if abs(r) > abs(best_r):
                    best_r = r
                    best_lag = lag
                    best_p = p_val
            
            # Only record somewhat significant correlations (r > 0.3 or r < -0.3)
            # Weather variable original name mapping
            var_name = "temp_max_c" if "temp" in weather_col else "precipitation_mm"
            
            if abs(best_r) >= 0.25 and best_p <= 0.10:
                results.append({
                    "product_category": category,
                    "pincode": pin,
                    "weather_variable": var_name,
                    "correlation_r": round(best_r, 4),
                    "optimal_lag_days": best_lag,
                    "p_value": round(best_p, 6),
                    "computed_date": datetime.now().date()
                })
    
    # 3. Write results to PostgreSQL
    if results:
        res_df = pd.DataFrame(results)
        
        # To avoid unbounded growth, we could delete old correlations or just UPSERT
        # For simplicity in Phase 34, we truncate and overwrite the current active correlations.
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE weather_correlation_coefficients")
            res_df.to_sql('weather_correlation_coefficients', conn, if_exists='append', index=False)
            
        logger.info(f"Successfully computed and stored {len(res_df)} significant correlations.")
    else:
        logger.info("No statistically significant weather-price correlations found this month.")

def main():
    parser = argparse.ArgumentParser(description="Model 3: Weather-Price Correlation")
    parser.parse_args()

    engine = create_engine(DB_URI)
    try:
        compute_monthly_correlations(engine)
    except Exception as e:
        logger.error(f"Correlation job failed: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
