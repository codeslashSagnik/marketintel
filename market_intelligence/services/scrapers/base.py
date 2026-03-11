"""
BaseScraper — Abstract base class for all platform scrapers.

Provides the shared orchestration loop (run), shared config (CITY_CONFIG,
PRODUCT_SCHEMA, directories), and Kafka dual-write. Each platform scraper
inherits from this and implements its own set_location, discover_catalog,
and scrape_products methods.
"""
import os, time, random, logging, traceback
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

from market_intelligence.services.scrapers.browser import BrowserManager, PageScroller
from market_intelligence.services.scrapers.kafka_producer import KafkaProducerWrapper

logger = logging.getLogger("scrapers.base")


def get_logger(name: str) -> logging.Logger:
    """Return a pre-configured logger. Used by all scraper submodules."""
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        log.addHandler(sh)
    return log


# ── Shared Directories ────────────────────────────────────────
PROJECT_ROOT = Path("E:/cv projects/real_time-market-intelligence")
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
DEBUG_DIR    = DATA_DIR / "debug"
CACHE_DIR    = DATA_DIR / "cache"
LOGS_DIR     = PROJECT_ROOT / "logs"

for d in [RAW_DIR, DEBUG_DIR, CACHE_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Shared City Config (30 pincodes × 5 cities) ──────────────
CITY_CONFIG = {
    "kolkata": {
        "central":     ["700001", "700020"],
        "residential": ["700064", "700091"],
        "peripheral":  ["700084", "700104"]
    },
    "mumbai": {
        "central":     ["400001", "400021"],
        "residential": ["400053", "400067"],
        "peripheral":  ["400706", "400709"]
    },
    "delhi": {
        "central":     ["110001", "110011"],
        "residential": ["110085", "110075"],
        "peripheral":  ["110041", "110043"]
    },
    "bangalore": {
        "central":     ["560001", "560025"],
        "residential": ["560037", "560102"],
        "peripheral":  ["560067", "560105"]
    },
    "pune": {
        "central":     ["411001", "411004"],
        "residential": ["411014", "411057"],
        "peripheral":  ["412105", "412308"]
    },
}

# ── Shared Product Schema ─────────────────────────────────────
PRODUCT_SCHEMA = [
    "source", "city", "zone", "pincode", "category", "subcategory",
    "product_name", "brand", "variant", "current_price", "mrp",
    "discount_percent", "in_stock", "rating", "pack_size",
    "image_url", "scraped_at"
]


class BaseScraper(ABC):
    """Abstract base class for all platform scrapers."""

    # Subclasses must set this
    SOURCE: str = ""

    def __init__(self, headless: bool = True, max_prod: int = 200,
                 max_cat: int = 5, max_sub: int = 15):
        self.headless = headless
        self.max_prod = max_prod
        self.max_cat = max_cat
        self.max_sub = max_sub
        self.bm = BrowserManager(headless=headless)
        self.ps = PageScroller()
        self.kafka = KafkaProducerWrapper(source=self.SOURCE)
        self.logger = logging.getLogger(f"scrapers.{self.SOURCE}")

    @abstractmethod
    def set_location(self, driver, pincode: str) -> bool:
        """Set delivery location on the platform. Platform-specific."""
        ...

    @abstractmethod
    def discover_catalog(self, driver) -> List[Dict]:
        """Discover the product category tree. Returns list of scraping nodes."""
        ...

    @abstractmethod
    def scrape_page(self, driver, url: str, cat_name: str, subcat_name: str,
                    city: str, zone: str, pincode: str,
                    filters: Optional[Dict] = None) -> List[Dict]:
        """Navigate to a URL, apply filters, scroll, parse products. Platform-specific."""
        ...

    def discover_catalog_cached(self, driver, cache_max_age_hours: int = 24) -> list:
        """Load catalog from cache if fresh, otherwise discover and save."""
        import json
        from datetime import timedelta

        cache_file = CACHE_DIR / f"catalog_cache_{self.SOURCE}.json"

        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                cached_at = datetime.fromisoformat(cached["timestamp"])
                if datetime.utcnow() - cached_at < timedelta(hours=cache_max_age_hours):
                    catalog = cached["catalog"]
                    self.logger.info(f"Loaded {len(catalog)} catalog nodes from cache.")
                    return catalog
                else:
                    self.logger.info("Cache expired, re-discovering catalog.")
            except Exception as e:
                self.logger.warning(f"Cache read failed: {e}")

        catalog = self.discover_catalog(driver)

        if catalog:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump({"timestamp": datetime.utcnow().isoformat(), "catalog": catalog}, f, indent=2)
                self.logger.info(f"Saved {len(catalog)} catalog nodes to cache.")
            except Exception as e:
                self.logger.warning(f"Cache write failed: {e}")

        return catalog

    def _publish_and_save(self, records: list, city: str, pincode: str):
        """Publish scraped records to Kafka only (no CSV output)."""
        if not records:
            return
        self.kafka.publish_batch(records)
        self.logger.info(f"Published {len(records)} records from {city}/{pincode} to Kafka")

    def run(self, city_config: dict = None) -> pd.DataFrame:
        """
        Main orchestration loop — identical for all scrapers.

        1. Discover catalog once globally
        2. For each city → zone → pincode:
           a. Create/restart browser (anti-bot)
           b. Set location
           c. Scrape all catalog nodes
           d. Dual-write CSV + Kafka
        """
        if city_config is None:
            city_config = CITY_CONFIG

        all_records = []
        t0 = time.monotonic()
        driver = None
        pincodes_scraped = 0

        # ── Phase 1: Discover catalog ONCE ────────────────────
        driver = self.bm.create_driver()
        first_city = list(city_config.keys())[0]
        first_pincode = list(city_config[first_city].values())[0][0]
        self.set_location(driver, first_pincode)
        catalog = self.discover_catalog_cached(driver)
        self.bm.quit(driver)
        driver = None

        self.logger.info(f"Global catalog: {len(catalog)} nodes. Starting multi-city scrape.")
        if not catalog:
            self.logger.error("Catalog discovery failed. Aborting.")
            return pd.DataFrame(columns=PRODUCT_SCHEMA)

        # ── Phase 2: Scrape all cities ────────────────────────
        for city, zones in city_config.items():
            self.logger.info(f"=== Starting city: {city} ===")

            for zone, pincodes in zones.items():
                for pincode in pincodes:
                    # Anti-bot: restart browser every 2 pincodes
                    if not driver or pincodes_scraped >= 2:
                        if driver:
                            self.logger.info("Anti-bot: Restarting browser.")
                            self.bm.quit(driver)
                        driver = self.bm.create_driver()
                        pincodes_scraped = 0

                    self.set_location(driver, pincode)
                    pincodes_scraped += 1

                    for cat_node in catalog:
                        cat_name = cat_node["category"]
                        subcat_name = cat_node["subcategory"]
                        url = cat_node["url"]
                        filters = {k: v for k, v in cat_node.items()
                                   if k not in ("category", "subcategory", "url")}

                        self.logger.info(f"[{city}|{zone}|{pincode}] {cat_name} -> {subcat_name}")
                        try:
                            records = self.scrape_page(
                                driver, url, cat_name, subcat_name,
                                city, zone, pincode, filters
                            )
                            if records:
                                self.logger.info(f"Parsed {len(records)} records")
                                all_records.extend(records)
                                self._publish_and_save(records, city, pincode)
                            else:
                                self.logger.warning(f"No records for {subcat_name}")

                            time.sleep(random.uniform(4, 7))
                        except Exception as e:
                            self.logger.error(f"Error: {e}\n{traceback.format_exc()}")
                            continue

        if driver:
            self.bm.quit(driver)
        self.kafka.close()

        df = pd.DataFrame(all_records, columns=PRODUCT_SCHEMA)
        self.logger.info(f"Pipeline finished in {time.monotonic() - t0:.1f}s. {len(df)} total items.")
        return df
