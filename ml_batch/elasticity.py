import os
import argparse
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_elasticity")

# ── Configuration ──────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB   = os.environ.get("PG_DB", "market_intel")
PG_USER = os.environ.get("PG_USER", "mi_admin")
PG_PASS = os.environ.get("PG_PASS", "market_intel_2026")

DB_URI = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

def estimate_price_elasticity(engine):
    """
    Monthly Batch Job: Estimates Price Elasticity using Stockouts as a proxy for Demand.
    Rationale: If a product is heavily discounted (ΔPrice < 0) and rapidly goes out of stock (ΔInStock < 0), 
               that product is highly elastic.
    """
    logger.info("Starting Price Elasticity Estimation (Monthly)...")
    
    # We look at historical sequences of price drops leading to stockouts
    # Simplified approach: Aggregate monthly max discount and stockout duration
    query = """
        SELECT 
            product_id, 
            pincode,
            DATE_TRUNC('day', scraped_at) as ds,
            AVG(discount_pct) as avg_discount,
            -- stockout ratio (1 = fully in stock, 0 = completely out that day)
            AVG(CASE WHEN in_stock THEN 1 ELSE 0 END) as stock_ratio 
        FROM fact_pricing_snapshots
        WHERE quality_flag != 'rejected'
          AND scraped_at >= NOW() - INTERVAL '90 days'
        GROUP BY product_id, pincode, DATE_TRUNC('day', scraped_at)
        HAVING COUNT(*) > 0
    """
    logger.info("Loading 90-day Pricing and Stock histories...")
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logger.warning("Insufficient pricing data to estimate elasticity.")
        return

    df['ds'] = pd.to_datetime(df['ds'])
    groups = df.groupby(['product_id', 'pincode'])
    
    predictions_to_insert = []
    
    for (prod_id, pin), group in groups:
        if len(group) < 30: # Need at least a month of data
            continue
            
        group = group.sort_values('ds')
        
        # Calculate daily change in discount and stock status
        group['discount_diff'] = group['avg_discount'].diff()
        group['stock_diff'] = group['stock_ratio'].diff()
        
        # Isolate days where discount increased significantly (>5%)
        # and observe if stock ratio dropped soon after (within 3 days)
        # For a simplified proxy: Correlation between discount and subsequent stockout
        # Invert stock_ratio to "stockout_ratio" (1 = completely out of stock)
        group['stockout_ratio'] = 1.0 - group['stock_ratio']
        
        from scipy.stats import pearsonr
        valid_mask = ~group['avg_discount'].isna() & ~group['stockout_ratio'].isna()
        
        x = group['avg_discount'][valid_mask]
        y = group['stockout_ratio'][valid_mask].shift(-2).fillna(0) # Did it go out of stock a couple days later?
        
        if len(x) < 10:
            continue
            
        # Pearson correlation: higher discount -> higher stockout ratio
        r, p_val = pearsonr(x, y)
        
        # We model Elasticity Index from -3 to 0.
        # Highly elastic: r approaches 1.0 (Higher discount -> high stockout ratio). We map r=1.0 to -3.
        # Inelastic: r approaches 0. We map r=0.0 to 0.
        
        # If r < 0, discounting reduces stockouts, which makes no sense (noise), so clamp to 0.
        elasticity = 0.0
        confidence = 0.0
        
        if r > 0 and p_val < 0.2:
            # Map r=[0,1] -> elasticity=[0, -3]
            elasticity = round(-3.0 * r, 2)
            # Map p_val=[0.2, 0] -> confidence=[0, 1]
            confidence = round(1.0 - (p_val / 0.2), 4)

        if elasticity < -0.5:  # Only log somewhat elastic items
            predictions_to_insert.append({
                "product_id": prod_id,
                "pincode": pin,
                "model_type": "elasticity_stockout_proxy",
                "elasticity_index": elasticity,
                "elasticity_confidence": confidence,
                "predicted_at": datetime.now()
            })
            
    if predictions_to_insert:
        preds_df = pd.DataFrame(predictions_to_insert)
        preds_df.to_sql('ml_predictions', engine, if_exists='append', index=False, method='multi')
        logger.info(f"Inserted {len(predictions_to_insert)} elasticity estimates into PostgreSQL.")
    else:
        logger.info("No significant elastic items found this month.")

def main():
    parser = argparse.ArgumentParser(description="Model 4: Price Elasticity")
    parser.parse_args()

    engine = create_engine(DB_URI)
    try:
        estimate_price_elasticity(engine)
    except Exception as e:
        logger.error(f"Elasticity job failed: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
