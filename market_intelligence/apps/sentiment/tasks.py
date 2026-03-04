"""
apps/sentiment/tasks.py

Celery task to fetch social/news sentiment from Reddit.
Runs every 15 minutes via Celery Beat.
"""
import time
import logging
from celery import shared_task
from django.utils import timezone

logger = logging.getLogger(__name__)

# Keywords / subreddits to monitor
KEYWORDS = ["grocery", "bigbasket", "blinkit", "zepto", "online grocery India"]


def _log_to_db(source, records, status, latency, error=""):
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
    name="apps.sentiment.tasks.fetch_sentiment",
    max_retries=3,
    default_retry_delay=60,
    queue="ingestion",
)
def fetch_sentiment(self):
    """
    Fetch Reddit posts for each keyword, score sentiment, and persist.

    Flow:
        1. Call RedditClient service per keyword
        2. Score each post (placeholder — swap for real NLP model)
        3. Bulk-create SentimentData rows
        4. Record metrics in IngestionLog
    """
    source = "reddit"
    logger.info("source=%s keywords=%s status=start", source, KEYWORDS)
    start = time.monotonic()

    try:
        from services.api_clients.reddit_client import RedditClient
        from apps.sentiment.models import SentimentData

        client = RedditClient()
        objs = []

        for keyword in KEYWORDS:
            posts = client.fetch_posts(keyword)         # returns list[dict]
            for post in posts:
                objs.append(
                    SentimentData(
                        source          = "reddit",
                        text            = post.get("text", ""),
                        sentiment_score = post.get("sentiment_score", 0.0),
                        keyword         = keyword,
                        created_at      = post.get("created_at", timezone.now()),
                    )
                )

        if objs:
            SentimentData.objects.bulk_create(objs, ignore_conflicts=True)

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
