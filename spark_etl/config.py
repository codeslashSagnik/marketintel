"""
Spark ETL Configuration — Centralized settings for the streaming pipeline.
"""
import os

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_PATTERN = "raw\\..*"            # Subscribe to all raw.* topics
KAFKA_ALERT_TOPIC = "alerts.price_drops"
KAFKA_MAX_OFFSETS = 1000                     # Max offsets per trigger

# ── PostgreSQL ────────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB   = os.environ.get("PG_DB", "market_intel")
PG_USER = os.environ.get("PG_USER", "mi_admin")
PG_PASS = os.environ.get("PG_PASS", "market_intel_2026")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

# ── Spark ─────────────────────────────────────────────────────
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoints/mi-etl")
TRIGGER_INTERVAL = "30 seconds"
WATERMARK_DELAY = "10 minutes"

# ── Spark Packages (for spark-submit) ────────────────────────
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.7.1",
])
