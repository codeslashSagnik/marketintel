"""
Shared Kafka Producer Wrapper — Publishes scraped records to Kafka.
Used by all scrapers (JioMart, BigBasket, Blinkit, Zepto).
Activation: Set env var KAFKA_BROKER. If not set, all publish calls are silent no-ops.
"""
import os, json, logging

logger = logging.getLogger("scrapers.kafka")


class KafkaProducerWrapper:
    """Publishes scraped records to Kafka in the unified schema."""

    # Map scraper field names → Kafka schema field names
    FIELD_MAP = {
        "category":         "category_l2",
        "subcategory":      "category_l3",
        "current_price":    "selling_price",
        "discount_percent": "discount_pct",
    }

    def __init__(self, source: str = "jiomart"):
        self.source = source
        self.topic = f"raw.{source}"
        self.producer = None
        self._delivery_errors = 0

        broker = os.environ.get("KAFKA_BROKER")
        if broker:
            try:
                from confluent_kafka import Producer
                self.producer = Producer({
                    "bootstrap.servers": broker,
                    "client.id": f"scraper-{source}",
                    "queue.buffering.max.messages": 10000,
                    "batch.size": 32768,
                    "linger.ms": 100,
                    "compression.type": "snappy",
                    "acks": "1",
                })
                logger.info(f"Kafka producer connected to {broker} → topic: {self.topic}")
            except Exception as e:
                logger.warning(f"Kafka producer init failed: {e}. Publishing disabled.")
                self.producer = None
        else:
            logger.info("KAFKA_BROKER not set — Kafka publishing disabled.")

    @property
    def enabled(self) -> bool:
        return self.producer is not None

    def _delivery_callback(self, err, msg):
        if err:
            self._delivery_errors += 1
            logger.warning(f"Kafka delivery failed: {err}")

    def _to_kafka_schema(self, record: dict) -> dict:
        """Convert scraper record to unified Kafka message schema."""
        msg = {}
        for old_key, val in record.items():
            new_key = self.FIELD_MAP.get(old_key, old_key)
            msg[new_key] = val
        msg["event_type"] = "product_price"
        msg.setdefault("product_url", None)
        return msg

    def publish_batch(self, records: list):
        """Publish a batch of scraper records to Kafka. No-op if disabled."""
        if not self.enabled or not records:
            return

        for record in records:
            msg = self._to_kafka_schema(record)
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=record.get("pincode", "").encode("utf-8"),
                    value=json.dumps(msg, default=str).encode("utf-8"),
                    callback=self._delivery_callback,
                )
            except BufferError:
                logger.warning("Kafka producer queue full, flushing...")
                self.producer.flush(timeout=5)
                self.producer.produce(
                    topic=self.topic,
                    key=record.get("pincode", "").encode("utf-8"),
                    value=json.dumps(msg, default=str).encode("utf-8"),
                    callback=self._delivery_callback,
                )

        self.producer.flush(timeout=10)
        logger.info(f"Published {len(records)} records to Kafka topic {self.topic}")

    def close(self):
        if self.producer:
            self.producer.flush(timeout=10)
            logger.info(f"Kafka producer closed. Delivery errors: {self._delivery_errors}")
