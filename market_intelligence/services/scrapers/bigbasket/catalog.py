import time
import random
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from typing import List, Dict

from services.scrapers.base import get_logger
from services.scrapers.bigbasket.config import TARGET_L1_CATEGORIES

logger = get_logger("services.scrapers.bigbasket.catalog")


class BBCatalogManager:
    """Dynamically discovers categories and subcategories from BigBasket."""

    def __init__(self):
        self.catalog = []

    def discover_catalog(self, driver) -> List[Dict[str, str]]:
        """Discover all categories and subcategories dynamically."""
        if not driver:
            logger.info("No driver provided — catalog discovery requires a live DOM.")
            return []

        logger.info("Starting dynamic category discovery...")
        categories = self._discover_top_categories(driver)
        if not categories:
            logger.warning("Dynamic discovery failed.")
            return []

        # Deduplicate categories by name (keep first match)
        seen_cats = set()
        unique_categories = []
        for cat_name, cat_url in categories:
            if cat_name not in TARGET_L1_CATEGORIES:
                continue
            if cat_name in seen_cats:
                continue
            seen_cats.add(cat_name)
            unique_categories.append((cat_name, cat_url))

        logger.info(f"Allowed categories: {[c[0] for c in unique_categories]}")

        catalog = []
        seen_urls = set()  # Deduplicate by URL across ALL categories
        for cat_name, cat_url in unique_categories:
            subcats = self._discover_subcategories(driver, cat_name, cat_url)
            if subcats:
                for sub_name, sub_url in subcats:
                    # Normalize URL for dedup (strip query params and trailing slash)
                    norm_url = sub_url.split('?')[0].rstrip('/')
                    if norm_url in seen_urls:
                        continue
                    seen_urls.add(norm_url)
                    catalog.append({
                        "category": cat_name,
                        "subcategory": sub_name,
                        "url": sub_url,
                    })
            else:
                catalog.append({
                    "category": cat_name,
                    "subcategory": "All",
                    "url": cat_url,
                })

        logger.info(f"Dynamic catalog: {len(catalog)} target nodes across {len(unique_categories)} categories.")
        self.catalog = catalog
        return catalog

    def _discover_top_categories(self, driver) -> list:
        """Hover over 'Shop by Category' and extract category links."""
        try:
            cat_btn = None
            btn_selectors = [
                (By.XPATH, "//button[.//span[contains(text(),'Category')]]"),
                (By.CSS_SELECTOR, "button[id^='headlessui-menu-button-']"),
            ]
            for by, sel in btn_selectors:
                elements = driver.find_elements(by, sel)
                for e in elements:
                    if e.is_displayed() and "category" in e.text.strip().lower():
                        cat_btn = e
                        break
                if cat_btn:
                    break

            if not cat_btn:
                logger.error("Could not find 'Shop by Category' button.")
                return []

            # Hover to open mega-menu
            ActionChains(driver).move_to_element(cat_btn).perform()
            time.sleep(2)

            menu_selectors = [
                "div[data-headlessui-state='open'] a",
                "div[role='menu'] a",
                "ul[role='menu'] a",
                "div[id^='headlessui-menu-items'] a",
            ]
            links = []
            for sel in menu_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elements if e.is_displayed()]
                if visible:
                    links = visible
                    break

            if not links:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                for a in soup.select("a[href*='/cl/']"):
                    href = a.get("href", "")
                    text = a.text.strip()
                    if text and href and "/cl/" in href:
                        full_url = f"https://www.bigbasket.com{href}" if href.startswith("/") else href
                        links.append((text, full_url))
                if links:
                    logger.info(f"Found {len(links)} categories via page source parsing.")
                    return links
                logger.warning("No category links found in mega-menu.")
                return []

            categories = []
            seen = set()
            for link in links:
                text = link.text.strip()
                href = link.get_attribute("href") or ""
                if text and href and text not in seen:
                    seen.add(text)
                    categories.append((text, href))

            logger.info(f"Found {len(categories)} top-level categories.")
            return categories

        except Exception as e:
            logger.error(f"Error discovering top categories: {e}")
            return []

    def _discover_subcategories(self, driver, cat_name: str, cat_url: str) -> list:
        """Navigate to a category page and extract subcategory links from the left sidebar."""
        try:
            driver.get(cat_url)
            time.sleep(random.uniform(3, 5))

            try:
                show_more_links = driver.find_elements(By.XPATH,
                    "//a[contains(text(),'Show more')] | //span[contains(text(),'Show more')] | //button[contains(text(),'Show more')]"
                )
                for sm in show_more_links:
                    if sm.is_displayed():
                        sm.click()
                        logger.info(f"Clicked 'Show more' in {cat_name}")
                        time.sleep(1)
            except:
                pass

            soup = BeautifulSoup(driver.page_source, "html.parser")

            slug_match = re.search(r'/(?:cl|pc)/([^/]+)/', cat_url)
            cat_slug = slug_match.group(1) if slug_match else ""

            subcats = []
            seen_hrefs = set()
            for a in soup.select("a[href*='/pc/']"):
                href = a.get("href", "")
                text = (a.string or a.get_text()).strip().strip('|').strip()
                if not text or not href or len(text) < 2:
                    continue

                if cat_slug and cat_slug not in href:
                    continue

                if "All Brands" in text or "View All" in text:
                    continue

                # Ignore top nav duplicates
                if "w-full" in a.parent.get("class", []):
                    continue

                norm_href = href.split('?')[0].rstrip('/')
                if norm_href not in seen_hrefs:
                    seen_hrefs.add(norm_href)
                    subcats.append((text, f"https://www.bigbasket.com{href}" if href.startswith("/") else href))

            logger.info(f"  {cat_name}: found {len(subcats)} subcategories")
            return subcats

        except Exception as e:
            logger.error(f"Error discovering subcategories for {cat_name}: {e}")
            return []
