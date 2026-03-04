"""
apps/ingestion/tasks.py

Celery tasks for competitor price scraping.
Each task: measures duration, logs structured output, saves to IngestionLog.
Retries up to 3 times on failure.
"""
import time
import logging
from celery import shared_task
from django.utils import timezone

logger = logging.getLogger(__name__)


def _log_to_db(source: str, records: int, status: str, latency: float, error: str = ""):
    """Defer import to avoid circular import at module load time."""
    from apps.monitoring.models import IngestionLog
    IngestionLog.objects.create(
        source=source,
        records_processed=records,
        status=status,
        latency_seconds=round(latency, 3),
        error_message=error,
    )


@shared_task(
    bind=True,
    name="apps.ingestion.tasks.scrape_competitor_prices",
    max_retries=3,
    default_retry_delay=60,   # back-off: 60s between retries
    queue="ingestion",
)
def scrape_competitor_prices(self):
    """
    Scrape competitor prices from BigBasket (and other platforms).
    Runs every 10 minutes via Celery Beat.

    Flow:
        1. Fetch raw data via BigBasketScraper service
        2. Bulk-create CompetitorPrice rows
        3. Record metrics in IngestionLog
    """
    source = "bigbasket_scraper"
    logger.info("source=%s status=start", source)
    start = time.monotonic()

    try:
        from services.scrapers.bigbasket_scraper import BigBasketScraper
        from apps.ingestion.models import CompetitorPrice

        scraper = BigBasketScraper()
        raw_records = scraper.fetch()                   # returns list[dict]

        if not raw_records:
            logger.warning("source=%s status=empty_response", source)
            _log_to_db(source, 0, "partial", time.monotonic() - start, "No records returned")
            return

        # Build model instances for bulk insert
        objs = [
            CompetitorPrice(
                product_id       = r["product_id"],
                platform         = r["platform"],
                price            = r["price"],
                original_price   = r.get("original_price"),
                discount_percent = r.get("discount_percent"),
                availability     = r.get("availability", True),
                scraped_at       = r.get("scraped_at", timezone.now()),
            )
            for r in raw_records
        ]
        CompetitorPrice.objects.bulk_create(objs, ignore_conflicts=True)

        latency = time.monotonic() - start
        logger.info(
            "source=%s records=%d duration=%.2fs status=success",
            source, len(objs), latency,
        )
        _log_to_db(source, len(objs), "success", latency)

    except Exception as exc:
        latency = time.monotonic() - start
        logger.error(
            "source=%s duration=%.2fs status=failed error=%s",
            source, latency, str(exc), exc_info=True,
        )
        _log_to_db(source, 0, "failed", latency, str(exc))
        raise self.retry(exc=exc)
