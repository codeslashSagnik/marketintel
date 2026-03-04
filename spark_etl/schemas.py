"""
Spark ETL Schemas — Unified message schema for all scraper sources.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, ArrayType
)

# ── Kafka Message Schema ──────────────────────────────────────
# Every scraper (JioMart, BigBasket, Blinkit, Zepto) must produce
# messages conforming to this schema.
PRODUCT_MESSAGE_SCHEMA = StructType([
    StructField("source",        StringType(),  True),
    StructField("event_type",    StringType(),  True),
    StructField("scraped_at",    StringType(),  True),   # ISO timestamp string
    StructField("city",          StringType(),  True),
    StructField("zone",          StringType(),  True),
    StructField("pincode",       StringType(),  True),
    StructField("category_l2",   StringType(),  True),
    StructField("category_l3",   StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("brand",         StringType(),  True),
    StructField("variant",       StringType(),  True),
    StructField("mrp",           DoubleType(),  True),
    StructField("selling_price", DoubleType(),  True),
    StructField("discount_pct",  DoubleType(),  True),
    StructField("in_stock",      BooleanType(), True),
    StructField("rating",        DoubleType(),  True),
    StructField("pack_size",     StringType(),  True),
    StructField("image_url",     StringType(),  True),
    StructField("product_url",   StringType(),  True),
])

# ── Weather Message Schema ──────────────────────────────────────
# The weather service produces a payload with an array of daily predictions.
WEATHER_DAY_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("type", StringType(), True),
    StructField("temp_max_c", DoubleType(), True),
    StructField("temp_min_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
])

WEATHER_MESSAGE_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("pincode", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("weather_data", ArrayType(WEATHER_DAY_SCHEMA), True),
])
