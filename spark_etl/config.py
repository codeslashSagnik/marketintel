"""
Spark ETL Configuration — Centralized settings for the streaming pipeline.
"""
from pathlib import Path
import os
from dotenv import load_dotenv

# Load .env relative to project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_PATTERN = "raw\\..*"            # Subscribe to all raw.* topics
KAFKA_ALERT_TOPIC = "alerts.price_drops"
KAFKA_MAX_OFFSETS = 1000                     # Max offsets per trigger

# ── PostgreSQL ────────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", os.environ.get("POSTGRES_HOST", "localhost"))
PG_PORT = os.environ.get("PG_PORT", os.environ.get("POSTGRES_PORT", "5433"))
PG_DB   = os.environ.get("PG_DB", os.environ.get("POSTGRES_DB", "marketintel"))
PG_USER = os.environ.get("PG_USER", os.environ.get("POSTGRES_USER", "postgres"))
PG_PASS = os.environ.get("PG_PASS", os.environ.get("POSTGRES_PASSWORD", "user"))

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

# ── Spark ─────────────────────────────────────────────────────
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", str(PROJECT_ROOT / "checkpoints"))
TRIGGER_INTERVAL = "30 seconds"
WATERMARK_DELAY = "10 minutes"

# ── Spark Packages (for spark-submit) ────────────────────────
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.7.1",
])
