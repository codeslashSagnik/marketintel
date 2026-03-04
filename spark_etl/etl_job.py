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
from pyspark.sql.functions import col, from_json

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
from spark_etl.sinks import write_pricing_facts, write_weather_facts, write_alerts_to_kafka, upsert_dimensions

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
        .select("data.*")
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

    spark = batch_df.sparkSession
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
        from pyspark.sql.functions import lit
        df = df.withColumn("price_change_pct", lit(None).cast("double"))

    try:
        upsert_dimensions(df)
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: dimension upsert failed: {e}")

    try:
        write_pricing_facts(df)
        logger.info(f"[Pricing] Batch {batch_id}: wrote facts successfully.")
    except Exception as e:
        logger.error(f"[Pricing] Batch {batch_id}: PostgreSQL write failed: {e}")

    try:
        write_alerts_to_kafka(df, threshold_pct=10.0)
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
