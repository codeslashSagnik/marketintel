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

from market_intelligence.services.scrapers.base import BaseScraper
from market_intelligence.services.scrapers.jiomart.config import JIOMART_CFG
from market_intelligence.services.scrapers.jiomart.location import JioMartLocationManager
from market_intelligence.services.scrapers.jiomart.catalog import JioMartCatalogManager
from market_intelligence.services.scrapers.jiomart.parser import JioMartProductParser

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
            logger.info(f"Expanding category: {cat_name}")
            grocery_lbl_xpath = f"//div[@data-attr='categories']//span[text()='{cat_name}']"
            grocery_lbl = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, grocery_lbl_xpath))
            )
            driver.execute_script("arguments[0].click();", grocery_lbl)
            time.sleep(4)
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
        """Click L4 subcategory filter — sidebar checkbox or modal."""
        try:
            logger.info(f"Applying L4 filter: {l4_filter_value}")
            val_escaped = l4_filter_value.replace("'", "\\'")
            xpath = f"//input[@name='attributes.category_level_4' and @value='{val_escaped}']"

            # Check if visible in sidebar
            elems = driver.find_elements(By.XPATH, xpath)
            clicked = False
            if elems and elems[0].is_displayed():
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elems[0])
                time.sleep(1)
                driver.execute_script("arguments[0].click();", elems[0])
                clicked = True
            else:
                # Open the modal
                try:
                    more_btn = driver.find_element(
                        By.CSS_SELECTOR,
                        "div[data-attr='attributes.category_level_4'] .show_more button"
                    )
                    if "none" not in more_btn.find_element(By.XPATH, "..").get_attribute("style"):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_btn)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", more_btn)
                        time.sleep(2)

                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.ID, "popup_filters"))
                        )

                        chk_input = driver.find_element(
                            By.XPATH, f"//ul[@id='popup_filters']{xpath}"
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chk_input)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", chk_input)

                        # Click Apply
                        try:
                            apply_btn = driver.find_element(By.ID, "filter_popup_apply")
                            driver.execute_script("arguments[0].click();", apply_btn)
                        except Exception as apply_err:
                            logger.warning(f"Failed clicking modal 'Apply': {apply_err}")

                        clicked = True
                except Exception as modal_err:
                    logger.warning(f"Failed opening modal: {modal_err}")

            if not clicked:
                logger.warning(f"Filter '{l4_filter_value}' not found in DOM.")

            time.sleep(5)
            logger.info("Filter applied successfully.")
        except Exception as e:
            logger.warning(f"Could not click L4 filter '{l4_filter_value}': {e}")
