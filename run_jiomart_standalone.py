"""
Standalone execution script for JioMart scraper.
Runs synchronously in the foreground terminal (no Celery/Redis required).
Safe to delete when no longer needed without affecting the main architecture.
"""
import logging
import sys
from market_intelligence.services.scrapers.jiomart.scraper import JioMartScraper
from market_intelligence.services.scrapers.base import CITY_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("jiomart_standalone")

def main():
    logger.info("🚀 Starting Standalone JioMart Scraper...")
    
    # Optional: If you want to test just one city to see it working quickly, uncomment below:
    # custom_config = {"mumbai": {"central": ["400001", "400021"]}}
    
    # We use the full CITY_CONFIG by default
    scraper = JioMartScraper(headless=True)
    
    # The run() method natively handles pushing to Kafka, bypassing Celery workers entirely.
    df = scraper.run(city_config=CITY_CONFIG)
    
    logger.info(f"✅ Scraping complete! {len(df)} records were published to Kafka.")
    logger.info("Spark ETL should have already ingested these and written them to PostgreSQL.")

if __name__ == "__main__":
    main()
