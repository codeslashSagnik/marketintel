-- ═══════════════════════════════════════════════════════════
--  Market Intelligence — ML Layer Schema
--  Tables for data quality, anomaly detection, forecasting,
--  correlation, elasticity, user profiling, and ranking.
-- ═══════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────
--  DATA QUALITY LOG
--  Every flagged/rejected row is logged here for auditing.
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_log (
    id               BIGSERIAL PRIMARY KEY,
    product_id       VARCHAR(64) NOT NULL,
    source_id        VARCHAR(20),
    pincode          VARCHAR(10),
    selling_price    DECIMAL(10,2),
    mrp              DECIMAL(10,2),
    quality_flag     VARCHAR(10) NOT NULL CHECK (quality_flag IN ('clean','flagged','rejected')),
    rejection_reason TEXT,
    snapshot_date    TIMESTAMP NOT NULL,
    evaluated_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_log_flag
    ON data_quality_log(quality_flag, snapshot_date DESC);

-- ───────────────────────────────────────────────────────────
--  ML PREDICTIONS  (per-event: forecast, anomaly, elasticity)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ml_predictions (
    id                      BIGSERIAL PRIMARY KEY,
    product_id              VARCHAR(64) NOT NULL,
    pincode                 VARCHAR(10) NOT NULL,

    -- Anomaly Detection (Model 2)
    point_anomaly_score     DECIMAL(5,4),
    trend_anomaly_score     DECIMAL(5,4),
    is_anomaly              BOOLEAN DEFAULT FALSE,
    anomaly_type            VARCHAR(20),       -- 'point', 'trend', 'both', NULL

    -- Price Forecasting (Model 1)
    model_type              VARCHAR(30),       -- 'prophet', 'lightgbm', NULL
    predicted_price_d1      DECIMAL(10,2),
    predicted_price_d2      DECIMAL(10,2),
    predicted_price_d3      DECIMAL(10,2),
    predicted_price_d4      DECIMAL(10,2),
    predicted_price_d5      DECIMAL(10,2),
    predicted_price_d6      DECIMAL(10,2),
    predicted_price_d7      DECIMAL(10,2),
    ci_lower                DECIMAL(10,2),     -- confidence interval lower
    ci_upper                DECIMAL(10,2),     -- confidence interval upper

    -- Elasticity (Model 4)
    elasticity_index        DECIMAL(5,2),      -- -3 (elastic) to 0 (inelastic)
    elasticity_confidence   DECIMAL(5,4),

    -- Metadata
    model_version           VARCHAR(30),
    predicted_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ml_pred_product
    ON ml_predictions(product_id, pincode, predicted_at DESC);

-- ───────────────────────────────────────────────────────────
--  WEATHER–PRICE CORRELATION (Model 3, monthly)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_correlation_coefficients (
    id                  BIGSERIAL PRIMARY KEY,
    product_category    VARCHAR(200) NOT NULL,
    pincode             VARCHAR(10) NOT NULL,
    weather_variable    VARCHAR(30) NOT NULL,
    correlation_r       DECIMAL(6,4),
    optimal_lag_days    SMALLINT,
    p_value             DECIMAL(8,6),
    computed_date       DATE DEFAULT CURRENT_DATE
);

CREATE INDEX IF NOT EXISTS idx_weather_corr_cat
    ON weather_correlation_coefficients(product_category, pincode);

-- ───────────────────────────────────────────────────────────
--  MODEL REGISTRY (version tracking for all models)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_registry (
    id              BIGSERIAL PRIMARY KEY,
    model_name      VARCHAR(100) NOT NULL,
    model_version   VARCHAR(30) NOT NULL,
    stage           VARCHAR(20) DEFAULT 'staging'
                        CHECK (stage IN ('staging','production','archived')),
    training_rows   INTEGER,
    date_range_from DATE,
    date_range_to   DATE,
    metric_mae      DECIMAL(10,4),
    metric_mape     DECIMAL(10,4),
    metric_precision DECIMAL(5,4),
    metric_recall   DECIMAL(5,4),
    metric_f1       DECIMAL(5,4),
    metric_r2       DECIMAL(6,4),
    trained_at      TIMESTAMP DEFAULT NOW(),
    promoted_at     TIMESTAMP,

    UNIQUE (model_name, model_version)
);

-- ───────────────────────────────────────────────────────────
--  USER SEARCH LOG & PROFILES (Models 6 & 7)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_search_log (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             VARCHAR(100) NOT NULL,
    product_category    VARCHAR(200),
    product_name        TEXT,
    intent              VARCHAR(50),
    pincode             VARCHAR(10),
    searched_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_profiles (
    user_id             VARCHAR(100) PRIMARY KEY,
    category_weights    JSONB,       -- {"vegetables": 0.45, "dairy": 0.28, ...}
    top_products        JSONB,       -- {"onion": 12, "tomato": 8, ...}
    primary_pincode     VARCHAR(10),
    last_updated        TIMESTAMP DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────
--  DAILY PRODUCT SCORES & USER SUGGESTIONS (Model 7)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_product_scores (
    id              BIGSERIAL PRIMARY KEY,
    product_id      VARCHAR(64) NOT NULL,
    pincode         VARCHAR(10) NOT NULL,
    score_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    value_score     DECIMAL(5,4),
    discount_score  DECIMAL(5,4),
    forecast_trend  DECIMAL(5,4),
    hist_value_score DECIMAL(5,4),
    anomaly_score   DECIMAL(5,4),

    UNIQUE (product_id, pincode, score_date)
);

CREATE TABLE IF NOT EXISTS user_daily_suggestions (
    id              BIGSERIAL PRIMARY KEY,
    user_id         VARCHAR(100) NOT NULL,
    rank            SMALLINT NOT NULL,
    product_id      VARCHAR(64) NOT NULL,
    pincode         VARCHAR(10) NOT NULL,
    source_id       VARCHAR(20),
    final_score     DECIMAL(5,4),
    reason_flags    JSONB,          -- ["high_value", "price_rising", ...]
    suggestion_date DATE NOT NULL DEFAULT CURRENT_DATE,

    UNIQUE (user_id, rank, suggestion_date)
);
