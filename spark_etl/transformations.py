"""
Spark ETL Transformations — Dedup, normalize, enrich, detect price changes.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, round as spark_round,
    when, current_timestamp, to_timestamp, explode, to_date,
    regexp_extract, lit
)


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Convert ISO string scraped_at to Spark TimestampType."""
    return df.withColumn("scraped_at", to_timestamp(col("scraped_at")))


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate records within the watermark window."""
    return df.dropDuplicates([
        "source", "pincode", "product_name", "variant", "scraped_at"
    ])


def normalize(df: DataFrame) -> DataFrame:
    """Standardize field values for consistent cross-platform comparison."""
    return df \
        .withColumn("brand", upper(trim(col("brand")))) \
        .withColumn("product_name", lower(trim(col("product_name")))) \
        .withColumn("discount_pct",
            when(col("mrp").isNotNull() & (col("mrp") > 0),
                 spark_round((col("mrp") - col("selling_price")) / col("mrp") * 100, 2)
            ).otherwise(col("discount_pct"))
        ) \
        .withColumn("processed_at", current_timestamp())


def drop_invalid(df: DataFrame) -> DataFrame:
    """Drop records with null product_name (invalid scrape)."""
    return df.filter(col("product_name").isNotNull())


def parse_pack_weight(df: DataFrame) -> DataFrame:
    """
    Parse the raw `pack_size` string into a numeric `pack_weight_g` (grams).
    Handles common patterns:
      '500 g', '500g', '500 gm' -> 500.0
      '1 kg', '1.5kg'           -> 1000.0, 1500.0
      '500 ml'                  -> 500.0  (treated as grams for liquids)
      '1 l', '1.5 ltr'          -> 1000.0, 1500.0
      '6 pcs', '12 N'           -> NULL   (count-based, no weight)
    """
    # Extract the numeric part and the unit part from pack_size
    df = df.withColumn("_ps_num",
        regexp_extract(col("pack_size"), r"([\d.]+)", 1).cast("double")
    )
    df = df.withColumn("_ps_unit",
        lower(trim(regexp_extract(col("pack_size"), r"[\d.]+\s*([a-zA-Z]+)", 1)))
    )

    # Convert to grams based on unit
    df = df.withColumn("pack_weight_g",
        when(col("_ps_unit").isin("g", "gm", "gms", "gram", "grams"), col("_ps_num"))
        .when(col("_ps_unit").isin("kg", "kgs"), col("_ps_num") * 1000)
        .when(col("_ps_unit").isin("ml", "mls"), col("_ps_num"))        # treat ml ~ g
        .when(col("_ps_unit").isin("l", "ltr", "litre", "liter"), col("_ps_num") * 1000)
        .otherwise(lit(None).cast("double"))
    )

    return df.drop("_ps_num", "_ps_unit")


def compute_unit_price(df: DataFrame) -> DataFrame:
    """Compute unit_price = selling_price / (pack_weight_g / 1000) → ₹ per kg."""
    return df.withColumn("unit_price",
        when(
            col("pack_weight_g").isNotNull() & (col("pack_weight_g") > 0) &
            col("selling_price").isNotNull(),
            spark_round(col("selling_price") / (col("pack_weight_g") / 1000), 2)
        ).otherwise(lit(None).cast("double"))
    )


def detect_price_changes(batch_df: DataFrame, latest_prices_df: DataFrame) -> DataFrame:
    """
    Join current batch against latest known prices from PostgreSQL.
    Computes price_change_pct for each record.
    
    Args:
        batch_df: Current micro-batch from Spark Structured Streaming
        latest_prices_df: Latest prices from PostgreSQL v_latest_prices view
    
    Returns:
        DataFrame with price_change_pct column added
    """
    # Select only the columns we need for the join
    prev = latest_prices_df.select(
        col("source").alias("prev_source"),
        col("pincode").alias("prev_pincode"),
        col("product_name").alias("prev_product"),
        col("variant").alias("prev_variant"),
        col("selling_price").alias("prev_price"),
    )
    
    joined = batch_df.join(
        prev,
        on=[
            batch_df["source"] == prev["prev_source"],
            batch_df["pincode"] == prev["prev_pincode"],
            batch_df["product_name"] == prev["prev_product"],
            batch_df["variant"] == prev["prev_variant"],
        ],
        how="left"
    ).drop("prev_source", "prev_pincode", "prev_product", "prev_variant")
    
    return joined.withColumn("price_change_pct",
        when(
            col("prev_price").isNotNull() & (col("prev_price") > 0),
            spark_round(
                (col("selling_price") - col("prev_price")) / col("prev_price") * 100, 2
            )
        ).otherwise(None)
    ).drop("prev_price")


def parse_weather_payload(df: DataFrame) -> DataFrame:
    """Explode the nested array of weather days into a flat factual format."""
    return df \
        .withColumn("weather_day", explode(col("weather_data"))) \
        .select(
            col("pincode"),
            col("scraped_at"),
            col("weather_day.date").alias("target_date"),
            col("weather_day.temp_max_c").alias("temp_max_c"),
            col("weather_day.temp_min_c").alias("temp_min_c"),
            col("weather_day.precipitation_mm").alias("precipitation_mm"),
            col("weather_day.wind_speed_kmh").alias("wind_kmh")
        ) \
        .withColumn("target_date", to_date(col("target_date"))) \
        .withColumn("scraped_at", to_timestamp(col("scraped_at")))

