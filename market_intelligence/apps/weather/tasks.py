"""
apps/weather/tasks.py

Celery task to fetch weather data from OpenWeatherMap.
Runs every 5 minutes via Celery Beat.
"""
import time
import logging
from celery import shared_task

logger = logging.getLogger(__name__)

# Cities to monitor — extend this list as needed
TARGET_CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune"]


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
    name="apps.weather.tasks.fetch_weather",
    max_retries=3,
    default_retry_delay=30,
    queue="ingestion",
)
def fetch_weather(self):
    """
    Fetch current weather for all target cities and store in WeatherData.

    Flow:
        1. Call WeatherClient service for each city
        2. Bulk-create WeatherData rows
        3. Record metrics in IngestionLog
    """
    source = "openweathermap"
    logger.info("source=%s cities=%s status=start", source, TARGET_CITIES)
    start = time.monotonic()

    try:
        from services.api_clients.weather_client import WeatherClient
        from apps.weather.models import WeatherData

        client = WeatherClient()
        objs = []

        for city in TARGET_CITIES:
            data = client.fetch_current(city)           # returns dict | None
            if data:
                objs.append(WeatherData(**data))
            else:
                logger.warning("source=%s city=%s status=no_data", source, city)

        if objs:
            WeatherData.objects.bulk_create(objs)

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
