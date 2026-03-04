#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  Kafka Topic Initialization
#  Runs inside the Kafka container after broker is ready.
# ═══════════════════════════════════════════════════════════

echo "⏳ Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:9092 1 60

echo "📦 Creating Kafka topics..."

# Raw scraper topics (one per source platform)
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic raw.jiomart \
    --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000  # 7 days

kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic raw.bigbasket \
    --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000

kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic raw.blinkit \
    --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000

kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic raw.zepto \
    --partitions 6 --replication-factor 1 \
    --config retention.ms=604800000

# Processed / enriched data
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic processed.products \
    --partitions 3 --replication-factor 1 \
    --config retention.ms=2592000000  # 30 days

# Alerts
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic alerts.price_drops \
    --partitions 1 --replication-factor 1 \
    --config retention.ms=7776000000  # 90 days

# Dead letter queue
kafka-topics --create --if-not-exists --bootstrap-server kafka:9092 \
    --topic dlq.failures \
    --partitions 1 --replication-factor 1 \
    --config retention.ms=2592000000  # 30 days

echo ""
echo "✅ All topics created. Listing:"
kafka-topics --list --bootstrap-server kafka:9092
