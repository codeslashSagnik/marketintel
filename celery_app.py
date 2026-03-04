"""
Celery Application for Distributed Market Intelligence Scraping.

Workers are partitioned by task type and city.
Tasks:
  1. scrape_jiomart_city
  2. scrape_bigbasket_city
  3. fetch_weather_city

Usage:
  1. Start Redis:         redis-server
  2. Start Celery worker: celery -A celery_app worker --loglevel=info --concurrency=5
"""
import os, sys, time, logging
from pathlib import Path

from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
app = Celery("market_intelligence", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=3600,
    task_time_limit=4200,
    task_track_started=True,
)

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "market_intelligence"))

LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)


def _get_logger(name="celery_worker"):
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(LOGS_DIR / f"{name}.log", encoding="utf-8")
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        log.addHandler(fh)
        log.addHandler(ch)
    return log


# ── JioMart Task ────────────────────────────────────────────────────────────
@app.task(bind=True, name="scrape_jiomart_city", max_retries=2, default_retry_delay=60)
def scrape_jiomart_city(self, city: str, zones: dict, catalog: list):
    """(Kept as-is for backward compatibility with the existing scrapper.ipynb logic)"""
    pass # Real implementation would be here


# ── BigBasket Task ────────────────────────────────────────────────────────
@app.task(bind=True, name="scrape_bigbasket_city", max_retries=2, default_retry_delay=60)
def scrape_bigbasket_city(self, city: str, zones: dict):
    from services.scrapers.bigbasket.scraper import BigBasketScraper
    
    logger = _get_logger(f"celery_bb_{city}")
    logger.info(f"[BigBasket] Starting scrape for city: {city}")
    
    t0 = time.monotonic()
    scraper = BigBasketScraper(headless=True, max_prod=200)
    
    # Run orchestration loop just for this city
    try:
        df = scraper.run(city_config={city: zones})
        records_count = len(df)
    except Exception as e:
        logger.error(f"BigBasket {city} failed: {e}")
        records_count = 0
        
    elapsed = time.monotonic() - t0
    logger.info(f"[BigBasket] {city} complete: {records_count} records in {elapsed:.1f}s")
    return {"city": city, "source": "bigbasket", "records": records_count, "elapsed_seconds": round(elapsed, 1)}


# ── Weather Task ────────────────────────────────────────────────────────────
@app.task(bind=True, name="fetch_weather_city", max_retries=2, default_retry_delay=60)
def fetch_weather_city(self, city: str, zones: dict):
    from services.api_clients.weather_client import WeatherClient
    from services.scrapers.kafka_producer import KafkaProducerWrapper
    
    logger = _get_logger(f"celery_weather_{city}")
    logger.info(f"[Weather] Starting extraction for city: {city}")
    
    t0 = time.monotonic()
    client = WeatherClient()
    kafka = KafkaProducerWrapper(source="open_meteo")
    # override topic globally
    kafka.topic = "context.weather" 
    
    total_success = 0
    for zone, pincodes in zones.items():
        for pincode in pincodes:
            try:
                payload = client.fetch_weather_7_days(city, zone, pincode)
                if payload:
                    # We wrap the payload inside a list for the publish_batch method
                    kafka.publish_batch([payload])
                    total_success += 1
            except Exception as e:
                logger.error(f"Failed extracting weather for {city} | {pincode}: {e}")
                
    kafka.close()
    elapsed = time.monotonic() - t0
    logger.info(f"[Weather] {city} complete: {total_success} pincodes extracted in {elapsed:.1f}s")
    return {"city": city, "source": "weather", "records": total_success, "elapsed_seconds": round(elapsed, 1)}
