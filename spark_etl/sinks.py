"""
Spark ETL Sinks — Write processed data to PostgreSQL Star Schema and Kafka alerts topic.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json, abs as spark_abs, md5, concat_ws, coalesce, lit
from spark_etl.config import JDBC_URL, JDBC_PROPERTIES, KAFKA_BROKER, KAFKA_ALERT_TOPIC
import psycopg2


def __get_pg_conn():
    return psycopg2.connect(
        host=JDBC_URL.split("//")[1].split(":")[0],
        port=JDBC_URL.split(":")[-1].split("/")[0],
        dbname=JDBC_URL.split("/")[-1],
        user=JDBC_PROPERTIES["user"],
        password=JDBC_PROPERTIES["password"],
    )


def upsert_dimensions(batch_df: DataFrame):
    """
    Update the `dim_product` and `dim_location` master catalogs.
    Uses a staging table + SQL UPSERT to handle inserts and updates.
    """
    if batch_df.isEmpty():
        return
    
    # Generate deterministic product_id (Hash of Name + Brand + Variant)
    # Use lit("") to avoid issues if column doesn't exist. Actually, category_l1 might not exist in all schemas.
    cols = batch_df.columns
    if "category_l1" not in cols:
        batch_df = batch_df.withColumn("category_l1", lit(None))
    
    dim_product = batch_df.withColumn(
        "product_id", 
        md5(concat_ws("|", 
            coalesce(col("product_name"), lit("")), 
            coalesce(col("brand"), lit("")), 
            coalesce(col("variant"), lit(""))
        ))
    ).select(
        "product_id", "product_name", "brand", "variant",
        "pack_size", "pack_weight_g",
        "category_l1", "category_l2", "category_l3",
        "image_url", "product_url"
    ).dropDuplicates(["product_id"])
    
    # Also extract dim_location
    dim_location = batch_df.select("pincode", "city", "zone").dropDuplicates(["pincode"])
    
    # ── Write dim_location staging ────────────────────────────────────
    dim_location.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "dim_location_staging") \
        .option("user", JDBC_PROPERTIES["user"]) \
        .option("password", JDBC_PROPERTIES["password"]) \
        .option("driver", JDBC_PROPERTIES["driver"]) \
        .mode("overwrite") \
        .save()

    # ── Write dim_product staging ─────────────────────────────────────
    dim_product.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "dim_product_staging") \
        .option("user", JDBC_PROPERTIES["user"]) \
        .option("password", JDBC_PROPERTIES["password"]) \
        .option("driver", JDBC_PROPERTIES["driver"]) \
        .mode("overwrite") \
        .save()
    
    # Execute UPSERTs
    conn = __get_pg_conn()
    try:
        with conn.cursor() as cur:
            # Upsert Locations
            cur.execute("""
                INSERT INTO dim_location (pincode, city, zone)
                SELECT pincode, city, zone FROM dim_location_staging
                ON CONFLICT (pincode)
                DO UPDATE SET city = EXCLUDED.city, zone = EXCLUDED.zone;
                
                DROP TABLE IF EXISTS dim_location_staging;
            """)
            
            # Upsert Products
            cur.execute("""
                INSERT INTO dim_product (
                    product_id, product_name, brand, variant,
                    pack_size, pack_weight_g,
                    category_l1, category_l2, category_l3, 
                    image_url, product_url, last_seen
                )
                SELECT 
                    product_id, product_name, brand, variant,
                    pack_size, pack_weight_g,
                    category_l1, category_l2, category_l3, 
                    image_url, product_url, NOW()
                FROM dim_product_staging
                ON CONFLICT (product_id)
                DO UPDATE SET
                    brand = EXCLUDED.brand,
                    pack_size = EXCLUDED.pack_size,
                    pack_weight_g = EXCLUDED.pack_weight_g,
                    category_l1 = EXCLUDED.category_l1,
                    category_l2 = EXCLUDED.category_l2,
                    category_l3 = EXCLUDED.category_l3,
                    image_url = EXCLUDED.image_url,
                    product_url = EXCLUDED.product_url,
                    last_seen = NOW();
                
                DROP TABLE IF EXISTS dim_product_staging;
            """)
        conn.commit()
    finally:
        conn.close()


def write_pricing_facts(batch_df: DataFrame):
    """Format and append pricing micro-batch to fact_pricing_snapshots."""
    if batch_df.isEmpty():
        return
        
    # Generate the product_id foreign key inline
    facts = batch_df.withColumn(
        "product_id", 
        md5(concat_ws("|", 
            coalesce(col("product_name"), lit("")), 
            coalesce(col("brand"), lit("")), 
            coalesce(col("variant"), lit(""))
        ))
    ).withColumnRenamed("source", "source_id") \
     .select(
        "product_id", "source_id", "pincode", 
        "mrp", "selling_price", "discount_pct", "in_stock", "rating",
        "price_change_pct", "unit_price", "scraped_at", "processed_at"
    )

    facts.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "fact_pricing_snapshots") \
        .option("user", JDBC_PROPERTIES["user"]) \
        .option("password", JDBC_PROPERTIES["password"]) \
        .option("driver", JDBC_PROPERTIES["driver"]) \
        .mode("append") \
        .save()


def write_weather_facts(batch_df: DataFrame):
    """Append contextual weather micro-batch to fact_daily_weather."""
    if batch_df.isEmpty():
        return
        
    facts = batch_df.select(
        "pincode", "target_date", "temp_max_c", "temp_min_c", 
        "precipitation_mm", "wind_kmh", "scraped_at"
    )

    facts.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "fact_daily_weather") \
        .option("user", JDBC_PROPERTIES["user"]) \
        .option("password", JDBC_PROPERTIES["password"]) \
        .option("driver", JDBC_PROPERTIES["driver"]) \
        .mode("append") \
        .save()


def write_alerts_to_kafka(batch_df: DataFrame, threshold_pct: float = 10.0):
    """Filter significant price changes and publish to Kafka alerts topic."""
    alerts = batch_df.filter(
        col("price_change_pct").isNotNull() & 
        (spark_abs(col("price_change_pct")) > threshold_pct)
    )
    
    if alerts.isEmpty():
        return
    
    alerts \
        .select(
            col("pincode").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", KAFKA_ALERT_TOPIC) \
        .save()
