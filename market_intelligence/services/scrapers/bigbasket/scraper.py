import time
import random
from typing import List, Dict, Optional
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from services.scrapers.base import BaseScraper, get_logger
from services.scrapers.bigbasket.config import BIGBASKET_CFG
from services.scrapers.bigbasket.location import BBLocationManager
from services.scrapers.bigbasket.catalog import BBCatalogManager
from services.scrapers.bigbasket.parser import BBProductParser

logger = get_logger("services.scrapers.bigbasket.scraper")


class BigBasketScraper(BaseScraper):
    """
    BigBasket implementation of the Market Intelligence Base Scraper.
    Delegates element-level duties to modular classes.
    """
    SOURCE = "bigbasket"

    def __init__(self, headless: bool = True, max_prod: int = 200,
                 max_cat: int = 5, max_sub: int = 15):
        super().__init__(headless, max_prod, max_cat, max_sub)
        self.lm = BBLocationManager(BIGBASKET_CFG)
        self.cm = BBCatalogManager()
        self.psr = BBProductParser(BIGBASKET_CFG, max_products=max_prod)

    def set_location(self, driver, pincode: str) -> bool:
        return self.lm.set_location(driver, pincode)

    def discover_catalog(self, driver) -> List[Dict]:
        return self.cm.discover_catalog(driver)

    def scrape_page(self, driver, url: str, cat_name: str, subcat_name: str,
                    city: str, zone: str, pincode: str,
                    filters: Optional[Dict] = None) -> List[Dict]:
        """Navigate to BigBasket subcategory and scrape the products."""
        driver.get(url)
        time.sleep(random.uniform(4, 7))

        actual_card_sel = "div[class*='SKUDeck___StyledDiv'], li[class*='sc-kIRgvC'], div[data-qa='product']"
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, actual_card_sel))
            )
        except TimeoutException:
            logger.warning(f"Product cards not found for {subcat_name}")
            return []

        # Scroll to load dynamically rendered cards
        self.ps.scroll_all(driver, actual_card_sel)

        # Parse the hydrated DOM
        records = self.psr.parse(driver, city, zone, pincode, cat_name, subcat_name)
        return records
