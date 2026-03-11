"""
Market Intelligence — PySpark Structured Streaming ETL Job

Reads raw product records from Kafka `raw.*` topics and context from `context.weather`.
Transforms them into a unified Star Schema, and sinks to PostgreSQL + Kafka alerts.

Usage:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    spark_etl/etl_job.py
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from spark_etl.config import (
    KAFKA_BROKER, KAFKA_TOPIC_PATTERN, KAFKA_MAX_OFFSETS,
    JDBC_URL, JDBC_PROPERTIES,
    CHECKPOINT_DIR, TRIGGER_INTERVAL, WATERMARK_DELAY,
)
from spark_etl.schemas import PRODUCT_MESSAGE_SCHEMA, WEATHER_MESSAGE_SCHEMA
from spark_etl.transformations import (
    parse_timestamps, deduplicate, normalize, drop_invalid, detect_price_changes,
    parse_weather_payload, parse_pack_weight, compute_unit_price
)
from spark_etl.ml_models import apply_data_quality_rules, detect_streaming_anomalies
from spark_etl.sinks import (
    write_pricing_facts, write_weather_facts, write_alerts_to_kafka, 
    upsert_dimensions, write_data_quality_logs, write_ml_predictions
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_etl")


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Kafka + PostgreSQL."""
    return SparkSession.builder \
        .appName("MarketIntelligence_ETL") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "6") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
        .getOrCreate()


# ── Pricing Stream Handlers ─────────────────────────────────────
def build_pricing_stream(spark: SparkSession):
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS) \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), PRODUCT_MESSAGE_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("scraped_at", to_timestamp(col("scraped_at")))

    return parsed


def process_pricing_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"[Pricing] Batch {batch_id}: empty.")
        return

    count = batch_df.count()
    logger.info(f"[Pricing] Batch {batch_id}: processing {count} records.")

    df = parse_timestamps(batch_df)
    df = drop_invalid(df)
    df = deduplicate(df)
    df = normalize(df)
    df = parse_pack_weight(df)
    df = compute_unit_price(df)
    
    # Generate product_id early for ML Models
    from pyspark.sql.functions import md5, concat_ws, coalesce, lit
    df = df.withColumn(
        "product_id", 
        md5(concat_ws("|", 
            coalesce(col("product_name"), lit("")), 
            coalesce(col("brand"), lit("")), 
            coalesce(col("variant"), lit(""))
        ))
    )

    spark = batch_df.sparkSession
    
    # 1. Price Change Detection
    try:
        latest_prices = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "v_latest_prices") \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .option("driver", JDBC_PROPERTIES["driver"]) \
            .load()

        df = detect_price_changes(df, latest_prices)
    except Exception as e:
        logger.warning(f"Price change detection skipped (DB unavailable?): {e}")
        df = df.withColumn("price_change_pct", lit(None).cast("double"))

    # 2. ML Layers (Data Quality + Anomaly Detection)
    try:
        # Fetch 30-day stats for statistical ML models
        stats_query = """
        (SELECT product_id AS stat_product_id, pincode AS stat_pincode, 
                AVG(selling_price) AS avg_price, STDDEV(selling_price) AS stddev_price 
         FROM fact_pricing_snapshots 
         WHERE scraped_at >= NOW() - INTERVAL '30 days' 
         GROUP BY product_id, pincode) AS stats
        """
        stats_df = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", stats_query) \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .option("driver", JDBC_PROPERTIES["driver"]) \
            .load()
    except Exception as e:
        logger.warning(f"Could not load 30d stats. Using empty proxy. {e}")
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("stat_product_id", StringType()),
            StructField("stat_pincode", StringType()),
            StructField("avg_price", DoubleType()),
            StructField("stddev_price", DoubleType())
        ])
        stats_df = spark.createDataFrame([], schema)

    # Apply Model 5: Data Quality
    df = apply_data_quality_rules(df, stats_df)
    
    try:
        write_data_quality_logs(df)
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: DQ Log write failed: {e}")

    # Isolate strictly 'clean' rows for downstream inference
    clean_df = df.filter(col("quality_flag") == "clean")
    
    # Apply Model 2: Streaming Anomaly Detection
    anomalies_df = detect_streaming_anomalies(clean_df, stats_df)

    try:
        write_ml_predictions(anomalies_df)
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: ML Predictions write failed: {e}")

    # 3. Sinks (only for non-rejected rows)
    facts_df = df.filter(col("quality_flag") != "rejected")

    try:
        upsert_dimensions(facts_df)
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: dimension upsert failed: {e}")

    try:
        write_pricing_facts(facts_df)
        logger.info(f"[Pricing] Batch {batch_id}: wrote facts successfully.")
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: PostgreSQL write failed: {e}")

    # Pass the anomaly DF explicitly to ensure alert topics only get genuine anomalies
    try:
        write_alerts_to_kafka(anomalies_df, threshold_pct=10.0)
    except Exception as e:
        logger.warning(f"[Pricing] Batch {batch_id}: alert publish failed: {e}")


# ── Weather Stream Handlers ─────────────────────────────────────
def build_weather_stream(spark: SparkSession):
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "context.weather") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS) \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), WEATHER_MESSAGE_SCHEMA).alias("data")) \
        .select("data.*")
    return parsed


def process_weather_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"[Weather] Batch {batch_id}: empty.")
        return

    count = batch_df.count()
    logger.info(f"[Weather] Batch {batch_id}: processing {count} payloads.")

    try:
        df = parse_weather_payload(batch_df)
        write_weather_facts(df)
        logger.info(f"[Weather] Batch {batch_id}: wrote {df.count()} daily facts.")
    except Exception as e:
        logger.error(f"[Weather] Batch {batch_id}: Transformation/Write failed: {e}")


def main():
    logger.info("Starting Market Intelligence Spark ETL (Star Schema Mode)...")
    spark = create_spark_session()

    # 1. Start Pricing Stream (raw.*)
    pricing_stream = build_pricing_stream(spark)
    pricing_wm = pricing_stream.withWatermark("scraped_at", WATERMARK_DELAY)
    
    pricing_query = pricing_wm.writeStream \
        .foreachBatch(process_pricing_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "pricing")) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()

    # 2. Start Weather Stream (context.weather)
    weather_stream = build_weather_stream(spark)
    
    weather_query = weather_stream.writeStream \
        .foreachBatch(process_weather_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "weather")) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()

    logger.info("Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
