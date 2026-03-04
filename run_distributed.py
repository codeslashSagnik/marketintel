"""
Distributed Orchestrator for Market Intelligence Scrapers.

Usage:
  # Dispatch all platform jobs for all cities
  python run_distributed.py --all

  # Dispatch only BigBasket
  python run_distributed.py --source bigbasket

  # Dispatch only Weather
  python run_distributed.py --source weather
"""
import argparse
import sys
import logging
from celery_app import scrape_jiomart_city, scrape_bigbasket_city, fetch_weather_city
from market_intelligence.services.scrapers.base import CITY_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("dispatcher")

def dispatch_jiomart():
    logger.info("Dispatching JioMart tasks (Not yet refactored - stub)")
    # catalog = discover() ...
    # for city, zones in CITY_CONFIG.items():
    #     scrape_jiomart_city.delay(city, zones, catalog)

def dispatch_bigbasket():
    logger.info("Dispatching BigBasket tasks across all cities...")
    for city, zones in CITY_CONFIG.items():
        logger.info(f" -> Queuing BigBasket task for {city}")
        scrape_bigbasket_city.delay(city, zones)

def dispatch_weather():
    logger.info("Dispatching Weather tasks across all cities...")
    for city, zones in CITY_CONFIG.items():
        logger.info(f" -> Queuing Weather task for {city}")
        fetch_weather_city.delay(city, zones)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dispatch scraping tasks to Celery workers.")
    parser.add_argument("--all", action="store_true", help="Run all scrapers")
    parser.add_argument("--source", type=str, choices=["jiomart", "bigbasket", "weather"], 
                        help="Run a specific scraper")
    
    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.all or args.source == "jiomart":
        dispatch_jiomart()
        
    if args.all or args.source == "bigbasket":
        dispatch_bigbasket()
        
    if args.all or args.source == "weather":
        dispatch_weather()

    logger.info("All selected tasks have been pushed to the Celery broker (Redis).")
