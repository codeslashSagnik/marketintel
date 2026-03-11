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
    Parse the raw `pack_size` string into:
      - `pack_value`  : the extracted numeric quantity (lower bound for ranges)
      - `pack_unit`   : the raw unit token as scraped (g, kg, ml, cm, pcs, tabs …)
      - `pack_weight_g`: quantity converted to grams for known weight/volume units;
                         NULL for unknown units (cm, pcs, tabs, etc.)

    Range and qualifier handling:
      '450-600 g'                   -> 450, 'g',  450.0
      '450g-600g'                   -> 450, 'g',  450.0
      '450g - 1 kg'                 -> 450, 'g',  450.0
      'Approx. 500g'                -> 500, 'g',  500.0
      '4 pcs - (Approx. 450 g)'    -> 450, 'g',  450.0
      '1.5 l'                       -> 1.5, 'l',  1500.0
      '30 cm'                       -> 30,  'cm', NULL
      '10 tabs'                     -> 10,  'tabs', NULL
      '6 pcs'                       -> 6,   'pcs', NULL
    """
    # Generic unit pattern: captures any alphabetic token immediately after a number.
    # Group 1 = first number (lower bound for ranges)
    # Group 2 = optional unit directly attached to first number (e.g. "450g-600g")
    # Group 3 = required trailing unit (handles "450-600 g" where unit comes at the end)
    pattern = (
        r"(\d+(?:\.\d+)?)"          # G1: first number
        r"\s*([a-z]+)?"             # G2: optional unit right after first number
        r"\s*(?:-|to)?\s*"          # optional range separator
        r"(?:\d+(?:\.\d+)?\s*)?"    # optional second number (skipped)
        r"([a-z]+)\b"              # G3: required trailing unit
    )

    df = df.withColumn("_ps_str", lower(trim(col("pack_size"))))

    df = df.withColumn("_ps_num",
        regexp_extract(col("_ps_str"), pattern, 1).cast("double")
    )
    df = df.withColumn("_ps_unit1",
        regexp_extract(col("_ps_str"), pattern, 2)   # unit glued to 1st number
    )
    df = df.withColumn("_ps_unit2",
        regexp_extract(col("_ps_str"), pattern, 3)   # trailing unit
    )

    # Prefer the immediately-attached unit; fall back to trailing unit
    df = df.withColumn("_ps_unit",
        when(col("_ps_unit1") != "", col("_ps_unit1"))
        .otherwise(col("_ps_unit2"))
    )

    # Store normalised raw unit for downstream ML / filtering
    df = df.withColumn("pack_unit",
        when(col("_ps_unit") != "", col("_ps_unit"))
        .otherwise(lit(None))
    )

    # Convert to grams only for known weight / volume units
    # All other units (cm, mm, pcs, tabs, sachets, …) yield NULL here
    # but their quantity and unit are captured in pack_value / pack_unit
    df = df.withColumn("pack_weight_g",
        when(col("_ps_unit").isin("g", "gm", "gms", "gram", "grams"),
             col("_ps_num"))
        .when(col("_ps_unit").isin("kg", "kgs"),
             col("_ps_num") * 1000)
        .when(col("_ps_unit").isin("mg"),
             col("_ps_num") / 1000)
        .when(col("_ps_unit").isin("ml", "mls"),
             col("_ps_num"))                          # ml ≈ g for water-based
        .when(col("_ps_unit").isin("l", "ltr", "litre", "liter"),
             col("_ps_num") * 1000)
        .when(col("_ps_unit").isin("cl"),
             col("_ps_num") * 10)                     # centilitres → ml
        .otherwise(lit(None).cast("double"))
    )

    # pack_value = raw extracted number (useful for non-weight units like cm, pcs)
    df = df.withColumn("pack_value",
        when(col("_ps_num").isNotNull() & (col("_ps_num") > 0), col("_ps_num"))
        .otherwise(lit(None).cast("double"))
    )

    return df.drop("_ps_str", "_ps_num", "_ps_unit", "_ps_unit1", "_ps_unit2")


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

