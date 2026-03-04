"""
config/celery.py

Celery application factory for the Market Intelligence Platform.
Registers all tasks and configures the Beat schedule.
"""

import os
import logging
from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger("celery")

# Point Django settings at our config module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("market_intelligence")

# Read Celery config from CELERY_* namespace in Django settings
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks from all INSTALLED_APPS
app.autodiscover_tasks()


# ── Periodic Beat Schedule ─────────────────────────────────────────────────────
app.conf.beat_schedule = {
    # Scrape competitor prices every 10 minutes
    "scrape-competitor-prices": {
        "task":     "apps.ingestion.tasks.scrape_competitor_prices",
        "schedule": crontab(minute="*/10"),
        "options":  {"queue": "ingestion"},
    },

    # Fetch weather data every 5 minutes
    "fetch-weather": {
        "task":     "apps.weather.tasks.fetch_weather",
        "schedule": crontab(minute="*/5"),
        "options":  {"queue": "ingestion"},
    },

    # Fetch social/news sentiment every 15 minutes
    "fetch-sentiment": {
        "task":     "apps.sentiment.tasks.fetch_sentiment",
        "schedule": crontab(minute="*/15"),
        "options":  {"queue": "ingestion"},
    },
}

app.conf.task_queues_default_exchange_type = "direct"


@app.task(bind=True)
def debug_task(self):
    """Health-check task – prints the current request info."""
    logger.info("Celery debug_task | request=%s", self.request)
    print(f"Request: {self.request!r}")
