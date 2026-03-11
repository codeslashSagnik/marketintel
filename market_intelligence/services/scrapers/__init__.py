"""
Scrapers package — production-grade platform scrapers.

Usage:
    from market_intelligence.services.scrapers.jiomart import JioMartScraper

    scraper = JioMartScraper(headless=True)
    df = scraper.run()
"""
from .base import BaseScraper, CITY_CONFIG, PRODUCT_SCHEMA


__all__ = ["BaseScraper", "CITY_CONFIG", "PRODUCT_SCHEMA"]
