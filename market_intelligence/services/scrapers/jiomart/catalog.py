"""JioMart-specific Category Discovery — L2 search sidebar + L4 modal subcategories."""
import time, random, logging, urllib.parse
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from market_intelligence.services.scrapers.jiomart.config import JIOMART_CFG

logger = logging.getLogger("scrapers.jiomart.catalog")


class JioMartCatalogManager:
    """Crawls L2 categories using the global Search sidebar and L4 filter block."""

    def __init__(self, max_categories: int = 5, max_subcategories: int = 15):
        self.cfg = JIOMART_CFG
        self.max_categories = max_categories
        self.max_subcategories = max_subcategories
        self.target_l1_name = "Groceries"

    def discover_catalog(self, driver) -> list:
        catalog = []

        target_l2_names = ['Dairy & Bakery', 'Fruits & Vegetables', 'Snacks & Branded Foods']
        logger.info(f"Using Search Discovery for {self.target_l1_name} across targets: {target_l2_names}")

        for l2 in target_l2_names:
            try:
                search_query = urllib.parse.quote(self.target_l1_name)
                l2_query = urllib.parse.quote(l2)
                search_url = f"{self.cfg['home_url'].rstrip('/')}/search?q={search_query}&searchtype=schedule&category_level_1={search_query}&category_level_2={l2_query}"

                logger.info(f"Navigating to L2 URL: {search_url}")
                driver.get(search_url)
                time.sleep(5)

                logger.info(f"Waiting for filter sidebar for {l2}...")
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.ID, "left_block"))
                )

                driver.execute_script("document.querySelectorAll('.location-backdrop').forEach(el => el.remove());")

                # Expand Sub Categories via +More button
                logger.info(f"Checking for '+More' modal under Sub Categories for {l2}...")
                modal_opened = False
                try:
                    more_btn = driver.find_element(By.CSS_SELECTOR, "div[data-attr='attributes.category_level_4'] .show_more button")
                    if "none" not in more_btn.find_element(By.XPATH, "..").get_attribute("style"):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_btn)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", more_btn)
                        time.sleep(3)

                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.ID, "popup_filters"))
                        )
                        modal_opened = True
                        logger.info(f"Opened subcategory modal for {l2}.")
                except Exception as e:
                    logger.info(f"No '+More' button found or failed to open for {l2}.")

                # Parse Subcategories
                soup = BeautifulSoup(driver.page_source, "html.parser")

                if modal_opened:
                    checkboxes = soup.select("li.popup-filters input.popup_refinement")
                else:
                    subcat_block = soup.select_one("div[data-attr='attributes.category_level_4']")
                    if subcat_block:
                        checkboxes = subcat_block.select("li.ais-RefinementList-item input.ais-refinement-list--checkbox")
                    else:
                        checkboxes = []

                logger.info(f"Found {len(checkboxes)} deep subcategories under {l2}.")

                for chk in checkboxes[:self.max_subcategories]:
                    val = chk.get("value")
                    if val:
                        catalog.append({
                            "category": l2,
                            "subcategory": val,
                            "url": search_url,
                            "l4_filter_value": val
                        })

            except Exception as e:
                logger.error(f"Discovery failed for L2 '{l2}': {e}")

        logger.info(f"Catalog discovery complete. Found {len(catalog)} mapping nodes across L2 targets.")
        return catalog
