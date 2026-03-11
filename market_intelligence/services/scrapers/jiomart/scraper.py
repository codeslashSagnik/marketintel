"""
JioMartScraper — Production scraper for JioMart.

Inherits from BaseScraper and implements:
  - set_location()     → Google Places area-search modal
  - discover_catalog() → L2 search sidebar + L4 modal subcategories
  - scrape_page()      → Navigate, apply L4 filter, scroll, parse DOM
"""
import time, random, logging
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from services.scrapers.base import BaseScraper
from services.scrapers.jiomart.config import JIOMART_CFG
from services.scrapers.jiomart.location import JioMartLocationManager
from services.scrapers.jiomart.catalog import JioMartCatalogManager
from services.scrapers.jiomart.parser import JioMartProductParser

logger = logging.getLogger("scrapers.jiomart")


class JioMartScraper(BaseScraper):
    """Production JioMart scraper — Selenium-based, full catalog."""

    SOURCE = "jiomart"

    def __init__(self, headless: bool = True, max_prod: int = 200,
                 max_cat: int = 1, max_sub: int = 15):
        super().__init__(headless=headless, max_prod=max_prod,
                         max_cat=max_cat, max_sub=max_sub)
        self.location_mgr = JioMartLocationManager()
        self.catalog_mgr = JioMartCatalogManager(
            max_categories=max_cat, max_subcategories=max_sub
        )
        self.parser = JioMartProductParser(max_products=max_prod)

    # ── Abstract method implementations ───────────────────────

    def set_location(self, driver, pincode: str) -> bool:
        return self.location_mgr.set_location(driver, pincode)

    def discover_catalog(self, driver) -> List[Dict]:
        return self.catalog_mgr.discover_catalog(driver)

    def scrape_page(self, driver, url: str, cat_name: str, subcat_name: str,
                    city: str, zone: str, pincode: str,
                    filters: Optional[Dict] = None) -> List[Dict]:
        """Navigate to category URL, apply L4 filter, scroll, and parse products."""

        l4_filter_value = (filters or {}).get("l4_filter_value")

        # Navigate to the category page
        logger.info(f"Navigating to {url}")
        driver.get(url)
        time.sleep(random.uniform(4, 7))

        # Remove location backdrops
        driver.execute_script(
            "document.querySelectorAll('.location-backdrop').forEach(el => el.remove());"
        )

        # Expand the parent category in the sidebar
        try:
            # Check if ANY subcategory is already visible before clicking expand
            is_expanded = False
            # Look for ANY L4 filter input in the sidebar that is displayed
            sidebar_l4_xpath = "//div[@data-attr='attributes.category_level_4']//input[@name='attributes.category_level_4']"
            sidebar_l4_elems = driver.find_elements(By.XPATH, sidebar_l4_xpath)
            if any(e.is_displayed() for e in sidebar_l4_elems):
                is_expanded = True

            if not is_expanded:
                logger.info(f"Expanding category: {cat_name}")
                grocery_lbl_xpath = f"//div[@data-attr='categories']//span[normalize-space(text())='{cat_name.strip()}']"
                grocery_lbl = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, grocery_lbl_xpath))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", grocery_lbl)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", grocery_lbl)
                time.sleep(5)
            else:
                logger.info(f"Category '{cat_name}' already expanded, skipping click.")
        except Exception as e:
            logger.warning(f"Could not expand main category '{cat_name}': {e}")

        driver.execute_script(
            "document.querySelectorAll('.location-backdrop').forEach(el => el.remove());"
        )

        # Apply L4 subcategory filter
        if l4_filter_value:
            self._apply_l4_filter(driver, l4_filter_value)

        # Scroll to load all products
        logger.info("Scrolling page...")
        self.ps.scroll_all(driver, JIOMART_CFG["product_card"])

        # Parse products from page
        logger.info("Parsing products...")
        records = self.parser.parse(driver, city, zone, pincode, cat_name, subcat_name)

        return records

    # ── Internal helper ───────────────────────────────────────

    def _apply_l4_filter(self, driver, l4_filter_value: str):
        """Click L4 subcategory filter — sidebar checkbox."""
        try:
            logger.info(f"Applying L4 filter: {l4_filter_value}")
            val_escaped = l4_filter_value.strip().replace("'", "\\'")
            xpath = f"//input[@name='attributes.category_level_4' and normalize-space(@value)='{val_escaped}']"

            # Check if visible in sidebar
            elems = driver.find_elements(By.XPATH, xpath)
            clicked = False
            if elems and any(e.is_displayed() for e in elems):
                target_elem = next(e for e in elems if e.is_displayed())
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_elem)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", target_elem)
                clicked = True

            if not clicked:
                logger.warning(f"Filter '{l4_filter_value}' not found in DOM.")

            time.sleep(5)
            logger.info("Filter applied successfully.")
        except Exception as e:
            logger.warning(f"Could not click L4 filter '{l4_filter_value}': {e}")
