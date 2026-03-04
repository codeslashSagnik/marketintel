-- ═══════════════════════════════════════════════════════════
--  Market Intelligence — PostgreSQL Star Schema Init
--  Auto-executed on first `docker compose up`
-- ═══════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ───────────────────────────────────────────────────────────
--  DIMENSION: Location
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    pincode VARCHAR(10) PRIMARY KEY,
    city    VARCHAR(50),
    zone    VARCHAR(30)
);

-- ───────────────────────────────────────────────────────────
--  DIMENSION: Source Platforms
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_source (
    source_id VARCHAR(20) PRIMARY KEY
);

INSERT INTO dim_source (source_id) VALUES 
('jiomart'), ('bigbasket'), ('blinkit'), ('zepto') 
ON CONFLICT DO NOTHING;

-- ───────────────────────────────────────────────────────────
--  DIMENSION: Master Product Catalog
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_id   VARCHAR(64) PRIMARY KEY, -- Deterministic MD5 hash of name|brand|variant
    product_name TEXT NOT NULL,
    brand        VARCHAR(200),
    variant      VARCHAR(200),
    pack_size    VARCHAR(100),             -- Raw string as scraped, e.g. "500 g", "1 kg", "6 pcs"
    pack_weight_g DECIMAL(10,2),           -- Normalized weight in grams (parsed from pack_size)
    category_l1  VARCHAR(200),
    category_l2  VARCHAR(200),
    category_l3  VARCHAR(200),
    image_url    TEXT,
    product_url  TEXT,
    first_seen   TIMESTAMP DEFAULT NOW(),
    last_seen    TIMESTAMP DEFAULT NOW()
);


-- ───────────────────────────────────────────────────────────
--  FACT: Pricing Snapshots (Partitioned by scraped_at)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_pricing_snapshots (
    id               BIGSERIAL,
    product_id       VARCHAR(64) NOT NULL,
    source_id        VARCHAR(20) NOT NULL,
    pincode          VARCHAR(10) NOT NULL,
    mrp              DECIMAL(10,2),
    selling_price    DECIMAL(10,2),
    discount_pct     DECIMAL(5,2),
    in_stock         BOOLEAN,
    rating           DECIMAL(3,1),
    price_change_pct DECIMAL(6,2),
    unit_price       DECIMAL(10,2),        -- Computed: selling_price / (pack_weight_g / 1000) = ₹ per kg
    scraped_at       TIMESTAMP NOT NULL,
    processed_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (id, scraped_at)
) PARTITION BY RANGE (scraped_at);

CREATE TABLE IF NOT EXISTS fact_pricing_2026_02 PARTITION OF fact_pricing_snapshots FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS fact_pricing_2026_03 PARTITION OF fact_pricing_snapshots FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS fact_pricing_2026_04 PARTITION OF fact_pricing_snapshots FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS fact_pricing_2026_05 PARTITION OF fact_pricing_snapshots FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS fact_pricing_2026_06 PARTITION OF fact_pricing_snapshots FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- ───────────────────────────────────────────────────────────
--  FACT: Daily Weather (Partitioned by target_date)
-- ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_daily_weather (
    id                 BIGSERIAL,
    pincode            VARCHAR(10) NOT NULL,
    target_date        DATE NOT NULL,
    temp_max_c         DECIMAL(5,2),
    temp_min_c         DECIMAL(5,2),
    precipitation_mm   DECIMAL(7,2),
    wind_kmh           DECIMAL(5,2),
    scraped_at         TIMESTAMP NOT NULL,
    PRIMARY KEY (id, target_date)
) PARTITION BY RANGE (target_date);

CREATE TABLE IF NOT EXISTS fact_weather_2026_02 PARTITION OF fact_daily_weather FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS fact_weather_2026_03 PARTITION OF fact_daily_weather FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS fact_weather_2026_04 PARTITION OF fact_daily_weather FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS fact_weather_2026_05 PARTITION OF fact_daily_weather FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');


-- ───────────────────────────────────────────────────────────
--  Indexes
-- ───────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_pricing_prod_src ON fact_pricing_snapshots(product_id, source_id, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_pricing_pincode  ON fact_pricing_snapshots(pincode, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_fact_weather_pincode  ON fact_daily_weather(pincode, target_date DESC);
CREATE INDEX IF NOT EXISTS idx_dim_product_lookup    ON dim_product(product_name, brand, variant);


-- ───────────────────────────────────────────────────────────
--  Business Views (Simulating the old flat tables for BI tools)
-- ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT DISTINCT ON (f.source_id, f.pincode, p.product_name, p.variant)
    f.source_id AS source,
    l.city,
    l.zone,
    f.pincode,
    p.product_name,
    p.brand,
    p.variant,
    p.category_l2,
    p.category_l3,
    p.pack_size,
    p.pack_weight_g,
    f.mrp,
    f.selling_price,
    f.discount_pct,
    f.unit_price,
    f.in_stock,
    f.rating,
    f.scraped_at
FROM fact_pricing_snapshots f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_location l ON f.pincode = l.pincode
ORDER BY f.source_id, f.pincode, p.product_name, p.variant, f.scraped_at DESC;

CREATE OR REPLACE VIEW v_price_comparison AS
SELECT
    product_name,
    brand,
    variant,
    source,
    city,
    pincode,
    selling_price,
    mrp,
    discount_pct,
    unit_price,
    pack_size,
    in_stock,
    scraped_at
FROM v_latest_prices
ORDER BY product_name, source;
