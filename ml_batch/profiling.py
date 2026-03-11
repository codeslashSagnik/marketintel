import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
import json
from sqlalchemy import create_engine, text

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ml_profiling")

# ── Configuration ──────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB   = os.environ.get("PG_DB", "market_intel")
PG_USER = os.environ.get("PG_USER", "mi_admin")
PG_PASS = os.environ.get("PG_PASS", "market_intel_2026")

DB_URI = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

def compute_user_profiles(engine):
    """
    Model 6: User Interest Profiler
    Aggregates user search logs, applying a time-decay weight to build an interest profile (category affinities).
    """
    logger.info("Computing Time-Decayed User Profiles...")
    
    # We use a 30-day lookback for search logs
    query = """
        SELECT 
            user_id, 
            product_category, 
            product_name, 
            searched_at,
            pincode
        FROM user_search_log
        WHERE searched_at >= NOW() - INTERVAL '30 days'
          AND product_category IS NOT NULL
    """
    df_searches = pd.read_sql(query, engine)
    
    if df_searches.empty:
        logger.info("No recent search logs to profile.")
        return
        
    df_searches['searched_at'] = pd.to_datetime(df_searches['searched_at'])
    now = pd.to_datetime(datetime.now())
    
    # Exponential decay function: weight halves every 7 days
    # w = e^(-lambda * t) where lambda = ln(2)/7
    import numpy as np
    halflife_days = 7.0
    decay_rate = np.log(2) / halflife_days
    
    df_searches['days_ago'] = (now - df_searches['searched_at']).dt.total_seconds() / (24 * 3600)
    df_searches['weight'] = np.exp(-decay_rate * df_searches['days_ago'])
    
    profiles_to_upsert = []
    
    for user, group in df_searches.groupby('user_id'):
        # Aggregate category weights
        cat_weights = group.groupby('product_category')['weight'].sum()
        # Normalize weights to sum to 1.0 (probabilities)
        cat_weights = cat_weights / cat_weights.sum()
        
        # Aggregate specific product interests (e.g. top 5 explicitly searched items)
        top_products_series = group.groupby('product_name')['weight'].sum().nlargest(5)
        
        # Primary Pincode (mode)
        primary_pin = group['pincode'].mode()[0] if not group['pincode'].empty else None
        
        profiles_to_upsert.append({
            "user_id": user,
            "category_weights": json.dumps(cat_weights.to_dict()),
            "top_products": json.dumps(top_products_series.to_dict()),
            "primary_pincode": primary_pin,
            "last_updated": datetime.now()
        })
        
    if profiles_to_upsert:
        df_profiles = pd.DataFrame(profiles_to_upsert)
        
        # Upsert logic for user_profiles
        with engine.begin() as conn:
            for _, row in df_profiles.iterrows():
                upsert_query = text("""
                    INSERT INTO user_profiles (user_id, category_weights, top_products, primary_pincode, last_updated)
                    VALUES (:uid, :cw, :tp, :pin, :upd)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET 
                        category_weights = EXCLUDED.category_weights,
                        top_products = EXCLUDED.top_products,
                        primary_pincode = EXCLUDED.primary_pincode,
                        last_updated = EXCLUDED.last_updated
                """)
                conn.execute(upsert_query, {
                    "uid": row['user_id'],
                    "cw": row['category_weights'],
                    "tp": row['top_products'],
                    "pin": row['primary_pincode'],
                    "upd": row['last_updated']
                })
        logger.info(f"Updated profiles for {len(profiles_to_upsert)} users.")

def compute_daily_value_scores(engine):
    """
    Model 7 Component A: Deterministic Value Scoring Engine.
    Examines all active products and calculates their intrinsic 'deal' value score today.
    Score components: Current Discount, Forecast trajectory, Historical Anomaly index.
    """
    logger.info("Computing Daily Value Scores across all products...")
    
    # We fetch the latest prices along with their associated predictions from ml_predictions
    query = """
        SELECT 
            p.product_id, 
            p.pincode,
            p.selling_price, 
            p.discount_pct, 
            m.predicted_price_d1, 
            m.predicted_price_d7,
            m.point_anomaly_score,
            m.elasticity_index
        FROM v_latest_prices p
        LEFT JOIN (
            -- Get latest prediction per product/pincode
            SELECT DISTINCT ON (product_id, pincode) *
            FROM ml_predictions
            ORDER BY product_id, pincode, predicted_at DESC
        ) m ON p.product_id = m.product_id AND p.pincode = m.pincode
        WHERE p.scraped_at >= NOW() - INTERVAL '2 days'
    """
    
    df = pd.read_sql(query, engine)
    if df.empty:
        logger.warning("No latest prices available to score.")
        return
        
    # Baseline Discount Score (0.0 to 1.0)
    # A 50% discount yields a score of 1.0. Higher discounts cap at 1.0.
    df['discount_score'] = (df['discount_pct'].fillna(0) / 50.0).clip(upper=1.0)
    
    # Forecast Trend Score
    # If price is predicted to GO UP next week, then buying today is HIGH value.
    # We calculate the delta percentage. E.g. expected 10% hike -> score of 1.0
    def calc_trend_score(row):
        if pd.isna(row['predicted_price_d1']) or pd.isna(row['predicted_price_d7']):
            return 0.0
        if row['predicted_price_d1'] <= 0: return 0.0
        
        delta_pct = ((row['predicted_price_d7'] - row['predicted_price_d1']) / row['predicted_price_d1']) * 100
        # If delta_pct > 0 (price going up), good time to buy. Max out at 10% hike.
        # If delta_pct < 0 (price going down), bad time to buy. (Score near 0).
        if delta_pct > 0:
            return min(delta_pct / 10.0, 1.0)
        return 0.0
        
    df['forecast_trend'] = df.apply(calc_trend_score, axis=1)
    
    # Historical Value Score / Anomaly Score (Is today anomalous on the down side?)
    # If selling_price dropped sharply today (positive anomaly), it's a structural deal.
    # anomaly_score is 0-1.
    df['anomaly_score'] = df['point_anomaly_score'].fillna(0.0).clip(upper=1.0)
    df['hist_value_score'] = df['anomaly_score'] * df['discount_score']
    
    # Determine Master Value Score (Weighted Average)
    # 50% Current Discount (What savings do I get now)
    # 30% Forecast Trend (Will it be more expensive tomorrow?)
    # 20% Historical Context (Is this actually a rare event?)
    df['value_score'] = (df['discount_score'] * 0.5) + (df['forecast_trend'] * 0.3) + (df['hist_value_score'] * 0.2)
    
    records = df[['product_id', 'pincode', 'value_score', 'discount_score', 'forecast_trend', 'hist_value_score', 'anomaly_score']].to_dict('records')
    for r in records:
        r['score_date'] = datetime.now().date()
        
    df_scores = pd.DataFrame(records)
    
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE daily_product_scores")
        df_scores.to_sql('daily_product_scores', conn, if_exists='append', index=False)
        
    logger.info(f"Computed Master Value Scores for {len(df_scores)} items.")


def generate_personalised_rankings(engine):
    """
    Model 7 Component B: Personalised Ranking.
    Matches the User Affinity Profile against the Daily Value Scores to generate the Top 10 lists per user.
    """
    logger.info("Generating Top 10 Personalised Watchlists...")
    
    # Load user profiles
    df_users = pd.read_sql("SELECT user_id, category_weights, primary_pincode FROM user_profiles", engine)
    
    # Load base scores
    query_scores = """
        SELECT 
            s.product_id, 
            s.pincode, 
            s.value_score,
            p.category_l1,
            v.source,
            v.product_name,
            v.discount_pct
        FROM daily_product_scores s
        JOIN dim_product p ON s.product_id = p.product_id
        JOIN v_latest_prices v ON s.product_id = v.product_id AND s.pincode = v.pincode
        WHERE s.value_score > 0.05
    """
    df_scores = pd.read_sql(query_scores, engine)
    
    if df_users.empty or df_scores.empty:
        logger.warning("Missing users or scores. Cannot generate watchlists.")
        return
        
    suggestions = []
    
    # Build suggestions per user
    for _, user in df_users.iterrows():
        uid = user['user_id']
        pincode = user['primary_pincode']
        
        # Parse affinity dictionary
        try:
            affinity = json.loads(user['category_weights']) if user['category_weights'] else {}
        except:
            affinity = {}
            
        # Filter scores to user's pincode
        if pincode:
            user_pool = df_scores[df_scores['pincode'] == pincode].copy()
        else:
            user_pool = df_scores.copy()
            
        if user_pool.empty:
            continue
            
        # Compute Personalised Final Score:
        # Base value_score * User Affinity Multiplier
        def calc_affinity_multiplier(category):
            # If user has searched this category, boost it (1.0 to 3.0 max)
            weight = affinity.get(category, 0.0)
            return 1.0 + (weight * 2.0)
            
        user_pool['affinity_mult'] = user_pool['category_l1'].apply(calc_affinity_multiplier)
        user_pool['final_score'] = user_pool['value_score'] * user_pool['affinity_mult']
        
        # Sort and take Top 10
        top_10 = user_pool.sort_values(by='final_score', ascending=False).head(10)
        
        for rank, (_, item) in enumerate(top_10.iterrows(), 1):
            
            # Formulate human-readable reason flags for the UI/LLM narrative
            flags = []
            if item['value_score'] > 0.5: flags.append("Excellent Deal")
            if item['discount_pct'] > 30: flags.append("Heavy Discount")
            if affinity.get(item['category_l1'], 0) > 0.2: flags.append("Based on your searches")
            
            suggestions.append({
                "user_id": uid,
                "rank": rank,
                "product_id": item['product_id'],
                "pincode": item['pincode'],
                "source_id": item['source'],
                "final_score": item['final_score'],
                "reason_flags": json.dumps(flags),
                "suggestion_date": datetime.now().date()
            })
            
    if suggestions:
        df_sugg = pd.DataFrame(suggestions)
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE user_daily_suggestions")
            df_sugg.to_sql('user_daily_suggestions', conn, if_exists='append', index=False)
            
        logger.info(f"Generated {len(df_sugg)} total personalised suggestions across users.")


def main():
    parser = argparse.ArgumentParser(description="Model 6 & 7: User Profiling & Ranking")
    parser.parse_args()

    engine = create_engine(DB_URI)
    try:
        compute_user_profiles(engine)
        compute_daily_value_scores(engine)
        generate_personalised_rankings(engine)
    except Exception as e:
        logger.error(f"Profiling job failed: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
