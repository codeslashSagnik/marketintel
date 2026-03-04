#!/usr/bin/env python
# coding: utf-8

# # 🧠 Real-Time Market Intelligence Engine — R&D Notebook
# ## 🛒 BigBasket Scraper (Selenium + Static Catalog)
#
# This notebook demonstrates the BigBasket scraping architecture with:
# - **Static category catalog** — hardcoded URLs, no dynamic discovery needed
# - **Area-based location search** — types area name into BigBasket's autocomplete
# - **React/Tailwind DOM parsing** — product cards with styled-components
#
# **Core Classes** (same pattern as JioMart):
# - `BrowserManager`: Headless Chrome, anti-detection, mouse jitter.
# - `BBLocationManager`: Sets location via BigBasket's area search modal.
# - `BBCatalogManager`: Returns the static catalog target map.
# - `PageScroller`: Scrolls infinite-scroll pages.
# - `BBProductParser`: Extracts product data from BigBasket's React cards.
# - `BBPipeline`: Orchestrates multi-city, multi-category scraping.

# In[1]:

import os, re, time, json, random, logging, traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# ── Directories ──────────────────────────────────────────────
PROJECT_ROOT = Path("E:/cv projects/real_time-market-intelligence")
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
DEBUG_DIR    = DATA_DIR / "debug"
CACHE_DIR    = DATA_DIR / "cache"
LOGS_DIR     = PROJECT_ROOT / "logs"

for d in [RAW_DIR, DEBUG_DIR, CACHE_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    log = logging.getLogger("bigbasket_notebook")
    if log.handlers: return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(thread)-12s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOGS_DIR / "bigbasket_rnd.log", encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(ch)
    return log

logger = _get_logger()
print("✅ Setup complete.")


# In[2]:

# ═══════════════════════════════════════════════════════════════
#  CELL 2 · CONFIGURATION
# ═══════════════════════════════════════════════════════════════
CHROME_VERSION = "145.0.7632.76"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

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

# BigBasket-specific config
BIGBASKET_CFG = {
    "source": "bigbasket",
    "home_url": "https://www.bigbasket.com/",

    # ── Location selectors ────────────────────────────────
    "location_btn": "button[id^='headlessui-menu-button']",
    "search_input": "input[placeholder*='Search for area']",
    "suggestion_item": "li[id^='headlessui-menu-item']",

    # ── Product card selectors ────────────────────────────
    # BigBasket uses React styled-components. These selectors
    # are best-effort — confirm in R&D diagnostic phase.
    "product_card": "div[data-qa='product']",
    "product_card_fallback": "li.SKUDeck, div.SKUDeck, div[class*='product-listing']",
    "product_name_sel": "h3, a[class*='ProductName'], [data-qa='product_name']",
    "brand_sel": "span[class*='BrandName'], [data-qa='brand_name']",
    "price_sel": "span[class*='discounted-price'], span.Pricing___StyledLabel, [data-qa='discount_price']",
    "mrp_sel": "span[class*='line-through'], span[style*='line-through'], [data-qa='actual_price']",
    "discount_sel": "div[class*='discount'], span[class*='OFF'], [data-qa='discount']",
    "variant_sel": "span[class*='PackChanger'], div[class*='pack-desc'], [data-qa='pack_desc']",
    "img_sel": "img[class*='ProductImage'], img[data-qa='product_image']",
    "out_of_stock_sel": "button[class*='notify'], span[class*='out-of-stock'], [data-qa='out_of_stock']",
    "load_more_btn": "button[class*='load-more'], button:has(svg[class*='arrow'])",
}

# ── Static Catalog: Fixed category → direct URL mapping ──────
BB_CATALOG = [
    {
        "category":    "Fruits & Vegetables",
        "subcategory": "All",
        "url": "https://www.bigbasket.com/cl/fruits-vegetables/?nc=nb",
    },
    {
        "category":    "Bakery, Cakes & Dairy",
        "subcategory": "Dairy",
        "url": "https://www.bigbasket.com/pc/bakery-cakes-dairy/dairy/?nc=nb",
    },
    {
        "category":    "Snacks & Branded Foods",
        "subcategory": "Biscuits & Cookies",
        "url": "https://www.bigbasket.com/pc/snacks-branded-foods/biscuits-cookies/?nc=nb",
    },
    {
        "category":    "Snacks & Branded Foods",
        "subcategory": "Breakfast Cereals",
        "url": "https://www.bigbasket.com/pc/snacks-branded-foods/breakfast-cereals/?nc=nb",
    },
    {
        "category":    "Snacks & Branded Foods",
        "subcategory": "Chocolates & Candies",
        "url": "https://www.bigbasket.com/pc/snacks-branded-foods/chocolates-candies/?nc=nb",
    },
    {
        "category":    "Snacks & Branded Foods",
        "subcategory": "Indian Mithai",
        "url": "https://www.bigbasket.com/pc/snacks-branded-foods/indian-mithai/?nc=nb",
    },
]

PRODUCT_SCHEMA = [
    "source", "city", "zone", "pincode", "category", "subcategory",
    "product_name", "brand", "variant", "current_price", "mrp",
    "discount_percent", "in_stock", "rating", "pack_size",
    "image_url", "scraped_at"
]

print("✅ Config ready.")


# In[3]:

# ═══════════════════════════════════════════════════════════════
#  CELL 3 · BROWSER MANAGER (identical to JioMart)
# ═══════════════════════════════════════════════════════════════
class BrowserManager:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.user_agent = random.choice(USER_AGENTS)

    @staticmethod
    def _resolve_driver() -> str:
        p = ChromeDriverManager(driver_version=CHROME_VERSION).install()
        if not p.endswith(".exe"):
            base = os.path.dirname(p) if os.path.isfile(p) else p
            for root, _, files in os.walk(base):
                if "chromedriver.exe" in files:
                    return os.path.join(root, "chromedriver.exe")
        return p

    def create_driver(self) -> webdriver.Chrome:
        opts = Options()
        if self.headless: opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(f"--user-agent={self.user_agent}")
        opts.add_argument("--window-size=1920,1080")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        for attempt in range(2):
            try:
                driver = webdriver.Chrome(service=Service(self._resolve_driver()), options=opts)
                driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": self.user_agent})
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                """})
                logger.info(f"Browser built (attempt {attempt+1}). Headless={self.headless}")
                return driver
            except Exception as e:
                logger.error(f"Failed browser init {attempt+1}: {e}")
                time.sleep(2)
        raise WebDriverException("Could not initialize browser.")

    @staticmethod
    def jitter_mouse(driver: webdriver.Chrome):
        try:
            driver.execute_script(f"""
                var evt = new MouseEvent('mousemove', {{
                    clientX: {random.randint(100, 800)},
                    clientY: {random.randint(100, 800)}
                }});
                document.dispatchEvent(evt);
            """)
        except: pass

    @staticmethod
    def quit(driver: webdriver.Chrome):
        try: driver.quit()
        except: pass


# In[4]:

# ═══════════════════════════════════════════════════════════════
#  CELL 4 · LOCATION MANAGER (BigBasket area search)
# ═══════════════════════════════════════════════════════════════
class BBLocationManager:
    """Sets BigBasket delivery location using the area-search autocomplete."""

    PINCODE_TO_SEARCH = {
        # Kolkata
        "700001": "BBD Bagh, Kolkata",
        "700020": "Park Street, Kolkata",
        "700064": "Salt Lake City, Kolkata",
        "700091": "New Town, Kolkata",
        "700084": "Behala, Kolkata",
        "700104": "Joka, Kolkata",
        # Mumbai
        "400001": "Fort, Mumbai",
        "400021": "Nariman Point, Mumbai",
        "400053": "Andheri West, Mumbai",
        "400067": "Kandivali, Mumbai",
        "400706": "Nerul, Navi Mumbai",
        "400709": "Vashi, Navi Mumbai",
        # Delhi
        "110001": "Connaught Place, Delhi",
        "110011": "Central Secretariat, Delhi",
        "110085": "Rohini, Delhi",
        "110075": "Dwarka, Delhi",
        "110041": "Najafgarh, Delhi",
        "110043": "Outer Delhi, Delhi",
        # Bangalore
        "560001": "MG Road, Bangalore",
        "560025": "Indiranagar, Bangalore",
        "560037": "Whitefield, Bangalore",
        "560102": "HSR Layout, Bangalore",
        "560067": "Hoskote, Bangalore",
        "560105": "Electronic City, Bangalore",
        # Pune
        "411001": "Camp, Pune",
        "411004": "Deccan, Pune",
        "411014": "Viman Nagar, Pune",
        "411057": "Hinjewadi, Pune",
        "412105": "Talegaon, Pune",
        "412308": "Loni Kalbhor, Pune",
    }

    def __init__(self, cfg: dict):
        self.cfg = cfg

    def set_location(self, driver: webdriver.Chrome, pincode: str) -> bool:
        area_query = self.PINCODE_TO_SEARCH.get(pincode, pincode)
        logger.info(f"Setting BB location for pincode {pincode} → '{area_query}'")

        # ── Step 1: Navigate to homepage ──────────────────────
        driver.get(self.cfg["home_url"])
        time.sleep(random.uniform(3, 5))

        # ── Step 2: Click the "Delivery in X mins / Select Location" button ──
        # IMPORTANT: BigBasket renders TWO identical location buttons
        # (one for mobile, one for desktop). The mobile one is hidden.
        # We MUST use find_elements (plural) and pick the VISIBLE one.
        try:
            loc_btn = None
            loc_selectors = [
                (By.XPATH, "//button[.//span[contains(text(),'Select Location')]]"),
                (By.XPATH, "//button[.//span[contains(text(),'Delivery in')]]"),
                (By.CSS_SELECTOR, "button.sc-gweoQa"),
                (By.CSS_SELECTOR, "button[id^='headlessui-menu-button-']"),
            ]
            for by, sel in loc_selectors:
                elements = driver.find_elements(by, sel)
                # Filter for visible elements only
                visible = [e for e in elements if e.is_displayed()]
                for v in visible:
                    btn_text = v.text.strip().lower()
                    # Skip "Shop by Category" or empty buttons
                    if "category" in btn_text or "shop" in btn_text or not btn_text:
                        continue
                    loc_btn = v
                    logger.info(f"Found VISIBLE location button via '{sel}': '{btn_text[:50]}'")
                    break
                if loc_btn:
                    break

            if not loc_btn:
                logger.error("Could not find visible location button on homepage.")
                return False

            driver.execute_script("arguments[0].click();", loc_btn)
            logger.info("Clicked location button — dropdown should open")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Location button click failed: {e}")
            return False


        # ── Step 3: Type area name in the dropdown's search input ──
        # The dropdown contains an input with placeholder "Search for area or street name"
        # Class: Input-sc-tvw4mq-0  (styled-component)
        # It lives inside a div with role="menu" (headlessui dropdown)
        try:
            search_input = None
            input_selectors = [
                "input[placeholder='Search for area or street name']",
                "input.Input-sc-tvw4mq-0",
                "div[role='menu'] input[type='text']",
                "div[id^='headlessui-menu-items'] input",
            ]
            # Wait for dropdown to appear, then find visible input
            time.sleep(1)
            for sel in input_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elements if e.is_displayed()]
                if visible:
                    search_input = visible[0]
                    logger.info(f"Found VISIBLE search input via '{sel}'")
                    break

            # Retry with short wait if not found immediately
            if not search_input:
                time.sleep(2)
                for sel in input_selectors:
                    elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    visible = [e for e in elements if e.is_displayed()]
                    if visible:
                        search_input = visible[0]
                        logger.info(f"Found VISIBLE search input via '{sel}' (retry)")
                        break

            if not search_input:
                logger.error("Could not find location search input in dropdown.")
                return False

            search_input.click()
            time.sleep(0.3)
            search_input.clear()

            # Type character by character (mimics human input)
            for ch in area_query:
                search_input.send_keys(ch)
                time.sleep(random.uniform(0.04, 0.10))
            logger.info(f"Typed area: '{area_query}'")
            time.sleep(2.5)
        except Exception as e:
            logger.warning(f"Could not type in location search box: {e}")
            return False

        # ── Step 4: Click first suggestion ───────────────────
        # Suggestions are <li> elements with class sc-jdkBTo inside the dropdown
        try:
            suggestion = None
            sugg_selectors = [
                "li.sc-jdkBTo",
                "li.cnPYAb",
                "div[role='menu'] ul li",
                "div[id^='headlessui-menu-items'] li",
            ]
            # Wait for suggestions to appear
            time.sleep(1)
            for sel in sugg_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elements if e.is_displayed()]
                if visible:
                    suggestion = visible[0]
                    logger.info(f"Found VISIBLE suggestion via '{sel}': '{suggestion.text.strip()[:50]}'")
                    break

            if suggestion:
                suggestion.click()
                logger.info(f"Clicked first suggestion for '{area_query}'")
                time.sleep(random.uniform(4, 6))
            else:
                # Fallback: press Enter
                logger.warning("No suggestion found, pressing Enter")
                search_input.send_keys(Keys.RETURN)
                time.sleep(4)
        except Exception as e:
            logger.warning(f"Suggestion click failed: {e}")
            return False

        # ── Step 5: Verify location was set ──────────────────
        # After selecting, the page reloads and the button text changes
        # from "Select Location" to the pincode/area name
        try:
            time.sleep(2)
            loc_btn_after = driver.find_element(
                By.XPATH,
                "//button[.//span[contains(text(),'Delivery in')] or .//span[contains(text(),'Get it')]]"
            )
            banner_text = loc_btn_after.text.strip()
            if "select location" not in banner_text.lower():
                logger.info(f"✅ Location set successfully! Banner: '{banner_text}'")
                return True
            else:
                logger.warning(f"Location may not be set. Banner still says: '{banner_text}'")
        except:
            pass

        logger.info(f"Location set for {pincode} (verification skipped)")
        return True



# In[5]:

# ═══════════════════════════════════════════════════════════════
#  CELL 5 · CATALOG MANAGER (Dynamic — discovers from Shop by Category)
# ═══════════════════════════════════════════════════════════════
class BBCatalogManager:
    """Dynamically discovers categories and subcategories from BigBasket.

    Flow:
      1. Hover "Shop by Category" to open the mega-menu
      2. Scrape top-level category links
      3. For each category page, scrape the left sidebar subcategory links
      4. Click "Show more +" if present to expand hidden subcategories
      5. Return full catalog: [{category, subcategory, url}, ...]
    """

    # Only scrape these categories (allowlist)
    ALLOWED_CATEGORIES = {
        "Fruits & Vegetables",
        "Bakery, Cakes & Dairy",
        "Snacks & Branded Foods",
    }

    def __init__(self):
        self.catalog = []

    def discover_catalog(self, driver: webdriver.Chrome = None) -> list:
        """Discover all categories and subcategories dynamically."""
        if not driver:
            # Fallback to static catalog if no driver
            logger.info(f"No driver provided — using static catalog ({len(BB_CATALOG)} nodes).")
            return BB_CATALOG

        logger.info("Starting dynamic category discovery...")
        categories = self._discover_top_categories(driver)
        if not categories:
            logger.warning("Dynamic discovery failed — falling back to static catalog.")
            return BB_CATALOG

        # Deduplicate categories by name (keep first match)
        seen_cats = set()
        unique_categories = []
        for cat_name, cat_url in categories:
            if cat_name not in self.ALLOWED_CATEGORIES:
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

    def _discover_top_categories(self, driver: webdriver.Chrome) -> list:
        """Hover over 'Shop by Category' and extract category links."""
        try:
            # Find the "Shop by Category" button
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
            from selenium.webdriver.common.action_chains import ActionChains
            ActionChains(driver).move_to_element(cat_btn).perform()
            time.sleep(2)

            # Extract category links from the mega-menu
            # The menu left column has category names as links or divs
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
                # Try parsing the page source for mega-menu links
                soup = BeautifulSoup(driver.page_source, "html.parser")
                # Look for links to /cl/ (category listing) pages
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
                # Only keep category-level links (e.g. /cl/fruits-vegetables/)
                if text and href and text not in seen:
                    seen.add(text)
                    categories.append((text, href))

            logger.info(f"Found {len(categories)} top-level categories.")
            return categories

        except Exception as e:
            logger.error(f"Error discovering top categories: {e}")
            return []

    def _discover_subcategories(self, driver: webdriver.Chrome, cat_name: str, cat_url: str) -> list:
        """Navigate to a category page and extract subcategory links from the left sidebar."""
        try:
            driver.get(cat_url)
            time.sleep(random.uniform(3, 5))

            # Click "Show more +" if present to expand all subcategories
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

            # Extract subcategory links from the left sidebar
            # Only keep links that belong to THIS category (match the URL slug)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Extract category slug from URL (e.g. /cl/fruits-vegetables/ → fruits-vegetables)
            import re as _re
            slug_match = _re.search(r'/(?:cl|pc)/([^/]+)/', cat_url)
            cat_slug = slug_match.group(1) if slug_match else ""
            logger.debug(f"Category slug: '{cat_slug}' from URL: {cat_url}")

            subcats = []
            seen_hrefs = set()  # Deduplicate by URL path
            # Find all /pc/ links on page and filter by parent category slug
            for a in soup.select("a[href*='/pc/']"):
                href = a.get("href", "")
                # Clean subcategory name: strip pipes, extra whitespace
                # Use .string or direct text to avoid doubled text from nested elements
                text = (a.string or a.get_text()).strip().strip('|').strip()
                # Only keep links whose URL contains the parent category slug
                if not text or not href or len(text) < 2:
                    continue
                if cat_slug and cat_slug not in href:
                    continue
                # Deduplicate by normalized URL path
                norm_href = href.split('?')[0].rstrip('/')
                if norm_href in seen_hrefs:
                    continue
                seen_hrefs.add(norm_href)
                full_url = f"https://www.bigbasket.com{href}" if href.startswith("/") else href
                subcats.append((text, full_url))

            logger.info(f"  {cat_name}: found {len(subcats)} subcategories")
            return subcats


        except Exception as e:
            logger.error(f"Error discovering subcategories for {cat_name}: {e}")
            return []

    def discover_catalog_cached(self, driver=None, cache_max_age_hours: int = 24) -> list:
        """Check cache, fallback to dynamic discovery."""
        cache_file = CACHE_DIR / "bb_catalog_cache.json"
        if cache_file.exists():
            age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_hours < cache_max_age_hours:
                with open(cache_file, "r") as f:
                    cached = json.load(f)
                logger.info(f"Using cached catalog ({len(cached)} nodes, {age_hours:.1f}h old)")
                return cached

        catalog = self.discover_catalog(driver)
        if catalog:
            with open(cache_file, "w") as f:
                json.dump(catalog, f, indent=2)
            logger.info(f"Cached catalog to {cache_file}")
        return catalog



# In[6]:

# ═══════════════════════════════════════════════════════════════
#  CELL 6 · PAGE SCROLLER (identical to JioMart)
# ═══════════════════════════════════════════════════════════════
class PageScroller:
    def __init__(self, max_scrolls: int = 60):
        self.max_scrolls = max_scrolls

    def scroll_all(self, driver: webdriver.Chrome, card_selector: str) -> int:
        last_count = 0
        stable_iters = 0
        steps = 0

        while steps < self.max_scrolls:
            driver.execute_script("window.scrollBy(0, window.innerHeight);")
            BrowserManager.jitter_mouse(driver)
            time.sleep(random.uniform(1.5, 3.0))

            # Try clicking "Load More" if visible
            try:
                load_btns = driver.find_elements(By.CSS_SELECTOR,
                    "button[class*='load-more'], button:has(> span:contains('Load More')), button[class*='showMore']"
                )
                for btn in load_btns:
                    if btn.is_displayed():
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
                        break
            except: pass

            new_count = len(driver.find_elements(By.CSS_SELECTOR, card_selector))
            if new_count == last_count:
                stable_iters += 1
                if stable_iters >= 2:
                    break
            else:
                stable_iters = 0
                last_count = new_count
            steps += 1

        driver.execute_script("window.scrollTo(0, 0);")
        return last_count


# In[7]:

# ═══════════════════════════════════════════════════════════════
#  CELL 7 · PRODUCT PARSER (BigBasket-specific DOM parsing)
# ═══════════════════════════════════════════════════════════════
class BBProductParser:
    """Parses BigBasket product cards from the listing page DOM.

    BigBasket uses React with styled-components. Class names are generated
    at build time, so we use a multi-strategy approach:
      1. Try data-qa attributes first (most stable)
      2. Fall back to CSS class patterns
      3. Final fallback: text-based heuristics
    """

    def __init__(self, cfg: dict, max_products: int = 200):
        self.cfg = cfg
        self.source = cfg["source"]
        self.max_products = max_products

    @staticmethod
    def _clean_price(val: str) -> Optional[float]:
        if not val: return None
        cleaned = re.sub(r"[^\d.]", "", str(val))
        try: return float(cleaned) if cleaned else None
        except: return None

    @staticmethod
    def _clean_perc(val: str) -> Optional[float]:
        if not val: return None
        cl = re.sub(r"[^\d.]", "", str(val))
        try: return float(cl) if cl else None
        except: return None

    def _find_product_cards(self, soup: BeautifulSoup) -> list:
        """Find product card elements using multiple selector strategies."""
        # Strategy 1: data-qa attribute
        cards = soup.select("div[data-qa='product']")
        if cards:
            logger.info(f"Found {len(cards)} cards via data-qa='product'")
            return cards

        # Strategy 2: Common BigBasket card patterns from latest DOM
        card_selectors = [
            "div[class*='SKUDeck___StyledDiv']",
            "li[class*='sc-kIRgvC']",
            "div.bFjDCO",
            "div.PaginateItems",
            "div[id^='sku-']",
            "div[data-qa='product']",
            "li[class*='PaginateItems']",
            "div[class*='SKUDeck']",
            "li.SKUDeck", "div.SKUDeck"
        ]
        for sel in card_selectors:
            cards = soup.select(sel)
            if cards:
                logger.info(f"Found {len(cards)} cards via '{sel}'")
                return cards

        # Strategy 3: Look for elements containing price-like spans
        # (last resort heuristic)
        potential = soup.select("div:has(> h3):has(span:contains('₹'))")
        if potential:
            logger.info(f"Found {len(potential)} cards via heuristic")
            return potential

        logger.warning("No product cards found with any strategy!")
        return []

    def _extract_text(self, item, selectors: str) -> Optional[str]:
        """Try comma-separated selectors to find text."""
        for sel in selectors.split(","):
            sel = sel.strip()
            el = item.select_one(sel)
            if el and el.text.strip():
                return el.text.strip()
        return None

    def _extract_image(self, item) -> Optional[str]:
        """Extract image URL from product card."""
        for sel in ["img[data-qa='product_image']", "img[class*='ProductImage']", "img"]:
            img = item.select_one(sel)
            if img:
                src = img.get("data-src") or img.get("src") or img.get("srcset", "").split(",")[0].split(" ")[0]
                if src and src.startswith("http"):
                    return src
        return None

    def parse(self, driver: webdriver.Chrome, city: str, zone: str, pincode: str,
              cat_name: str, subcat_name: str) -> List[dict]:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = self._find_product_cards(soup)
        ts = datetime.utcnow().isoformat() + "Z"

        records = []
        for item in items[:self.max_products]:
            try:
                # ── Product Name & Variant ──────────────────────────
                # Name is usually in an h3 tag
                name_el = item.select_one("h3.block, h3[class*='line-clamp'], h3")
                name = name_el.text.strip() if name_el else None
                variant = None

                if name and "-" in name:
                    # User rule: if hyphen present, everything after is the variant
                    # e.g., "Sweet Corn - Masala Masti" -> name="Sweet Corn", variant="Masala Masti"
                    parts = name.split("-", 1)
                    variant = parts[1].strip()

                # ── Brand ─────────────────────────────────
                brand_el = item.select_one("span[class*='BrandName'], span[class*='brand']")
                brand = brand_el.text.strip() if brand_el else None

                # ── Prices & Discount ────────────────────────────────
                mrp = None
                price = None
                discount = None

                # Look for all pricing elements (mrp has strike-through or is smaller, price is main)
                # In BigBasket DOM, price is usually the span with className containing 'Pricing' or 'Price'
                # and MRP is often a strike-through text (s, del) or a separate span next to it
                price_elements = item.select("span:contains('₹'), div:has(span:contains('₹'))")

                for el in price_elements:
                    text = el.text.strip()
                    if not text or '₹' not in text: continue

                    # Clean the value
                    val = self._clean_price(text.split('₹')[-1])
                    if not val: continue

                    # Is it strike-through (MRP)?
                    if el.name in ['s', 'del'] or el.find_parent(['s', 'del']) or \
                       'line-through' in el.get('style', '') or 'line-through' in el.get('class', []):
                        mrp = val
                    else:
                        # Assume it's the current price
                        # If we already have a price, and this new one is smaller, the old one was probably MRP
                        if price is not None:
                            if val < price:
                                mrp = price
                                price = val
                            elif val > price:
                                mrp = val
                        else:
                            price = val

                # If only one price found, it's both
                if price and not mrp:
                    mrp = price
                if mrp and not price:
                    price = mrp

                # ── Discount ──────────────────────────────
                disc_text = self._extract_text(item,
                    "span[class*='OFF'], span[class*='off'], div[class*='discount']"
                )
                if disc_text:
                    discount = self._clean_perc(disc_text)
                elif price and mrp and mrp > 0 and mrp != price:
                    discount = round((mrp - price) / mrp * 100, 2)

                # ── Rating ─────────────────────────────────
                rating = None
                rating_el = item.select_one("span:has(svg.Badges___StyledStarIcon-sc-1k3p1ug-0) > span")
                if rating_el:
                    rating = self._clean_perc(rating_el.text)

                # ── Pack Size ───────────────────
                pack_size = self._extract_text(item,
                    "span[class*='PackSelector___StyledLabel'], "
                    "[data-qa='pack_desc'], span[class*='PackChanger'], "
                    "span[class*='qty']"
                )

                # ── Image ─────────────────────────────────
                img_url = self._extract_image(item)

                # ── Stock status ──────────────────────────
                in_stock = True
                oos_text = self._extract_text(item,
                    "button[class*='notify'], span[class*='out-of-stock']"
                )
                if oos_text and ("notify" in oos_text.lower() or "out of stock" in oos_text.lower()):
                    in_stock = False

                # ── Compile record ──────────────────────────
                records.append({
                    "source": self.source,
                    "city": city,
                    "zone": zone,
                    "pincode": pincode,
                    "category": cat_name,
                    "subcategory": subcat_name,
                    "product_name": name,
                    "brand": brand,
                    "variant": variant,
                    "current_price": price,
                    "mrp": mrp,
                    "discount_perc": discount,
                    "in_stock": in_stock,
                    "rating": rating,
                    "pack_size": pack_size,
                    "image_url": img_url,
                    "scraped_at": ts
                })
            except Exception as e:
                logger.debug(f"Error parsing BB card: {e}")
                continue

        # Dedup within batch
        seen, deduped = set(), []
        for r in records:
            key = (r['product_name'], r['pincode'])
            if key not in seen:
                seen.add(key)
                deduped.append(r)

        logger.info(f"Parsed {len(deduped)} unique records (from {len(items)} cards)")
        return deduped


# In[8]:

# ═══════════════════════════════════════════════════════════════
#  CELL 8 · KAFKA PRODUCER (optional — shared with JioMart)
# ═══════════════════════════════════════════════════════════════
class KafkaProducerWrapper:
    """Publishes scraped records to Kafka. No-op if KAFKA_BROKER not set."""

    FIELD_MAP = {
        "category":         "category_l2",
        "subcategory":      "category_l3",
        "current_price":    "selling_price",
        "discount_percent": "discount_pct",
    }

    def __init__(self, source: str = "bigbasket"):
        self.source = source
        self.topic = f"raw.{source}"
        self.producer = None
        self._delivery_errors = 0

        broker = os.environ.get("KAFKA_BROKER")
        if broker:
            # Fix for Windows host running docker: 'kafka' hostname won't resolve.
            if broker == "kafka:9092" and os.name == 'nt':
                logger.warning("KAFKA_BROKER set to 'kafka:9092' on Windows. Falling back to 'localhost:9092'.")
                broker = "localhost:9092"
            try:
                from confluent_kafka import Producer
                self.producer = Producer({
                    "bootstrap.servers": broker,
                    "client.id": f"scraper-{source}",
                    "queue.buffering.max.messages": 10000,
                    "batch.size": 32768,
                    "linger.ms": 100,
                    "compression.type": "snappy",
                    "acks": "1",
                })
                logger.info(f"Kafka producer → {self.topic}")
            except Exception as e:
                logger.warning(f"Kafka init failed: {e}")
        else:
            logger.info("KAFKA_BROKER not set — publishing disabled.")

    @property
    def enabled(self) -> bool:
        return self.producer is not None

    def _delivery_callback(self, err, msg):
        if err:
            self._delivery_errors += 1

    def _to_kafka_schema(self, record: dict) -> dict:
        msg = {}
        for old_key, val in record.items():
            new_key = self.FIELD_MAP.get(old_key, old_key)
            msg[new_key] = val
        msg["event_type"] = "product_price"
        msg.setdefault("product_url", None)
        return msg

    def publish_batch(self, records: list):
        if not self.enabled or not records:
            return
        for record in records:
            msg = self._to_kafka_schema(record)
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=record.get("pincode", "").encode("utf-8"),
                    value=json.dumps(msg, default=str).encode("utf-8"),
                    callback=self._delivery_callback,
                )
            except BufferError:
                self.producer.flush(timeout=5)
                self.producer.produce(
                    topic=self.topic,
                    key=record.get("pincode", "").encode("utf-8"),
                    value=json.dumps(msg, default=str).encode("utf-8"),
                    callback=self._delivery_callback,
                )
        self.producer.flush(timeout=10)
        logger.info(f"Published {len(records)} → {self.topic}")

    def close(self):
        if self.producer:
            self.producer.flush(timeout=10)


# In[9]:

# ═══════════════════════════════════════════════════════════════
#  CELL 9 · ORCHESTRATION PIPELINE
# ═══════════════════════════════════════════════════════════════
class BBPipeline:
    """Orchestrates BigBasket scraping across all cities, zones, categories."""

    def __init__(self, headless: bool = True, max_prod: int = 200):
        self.headless = headless
        self.bm = BrowserManager(headless=headless)
        self.lm = BBLocationManager(BIGBASKET_CFG)
        self.cm = BBCatalogManager()
        self.ps = PageScroller()
        self.psr = BBProductParser(BIGBASKET_CFG, max_products=max_prod)
        self.kafka = KafkaProducerWrapper(source=BIGBASKET_CFG["source"])

    def run(self, city_config: dict = None) -> pd.DataFrame:
        if city_config is None:
            city_config = CITY_CONFIG

        all_records = []
        t0 = time.monotonic()
        driver = None
        pincodes_scraped = 0
        catalog = None  # Will be discovered dynamically on first iteration

        for city, zones in city_config.items():
            logger.info(f"=== Starting city: {city} ===")

            for zone, pincodes in zones.items():
                for pincode in pincodes:
                    # Anti-bot: restart browser every 2 pincodes
                    if not driver or pincodes_scraped >= 2:
                        if driver:
                            logger.info("Anti-bot: Restarting browser.")
                            self.bm.quit(driver)
                        driver = self.bm.create_driver()
                        pincodes_scraped = 0

                    success = self.lm.set_location(driver, pincode)
                    if not success:
                        logger.error(f"Failed to set location for {pincode}. Skipping this pincode.")
                        continue
                    pincodes_scraped += 1

                    # Discover catalog dynamically (only once, then cache)
                    if catalog is None:
                        catalog = self.cm.discover_catalog_cached(driver)
                        logger.info(f"Catalog: {len(catalog)} target nodes. Starting scrape.")

                    for cat_node in catalog:

                        cat_name = cat_node["category"]
                        subcat_name = cat_node["subcategory"]
                        url = cat_node["url"]

                        logger.info(f"[{city}|{zone}|{pincode}] {cat_name} → {subcat_name}")
                        try:
                            # Navigate to category page
                            driver.get(url)
                            time.sleep(random.uniform(4, 7))

                            # Wait for product cards to render (React hydration)
                            # IMPORTANT: Use the ACTUAL card classes from BigBasket's DOM
                            actual_card_sel = "div[class*='SKUDeck___StyledDiv'], li[class*='sc-kIRgvC'], div[data-qa='product']"
                            try:
                                WebDriverWait(driver, 15).until(
                                    EC.presence_of_element_located((
                                        By.CSS_SELECTOR,
                                        actual_card_sel
                                    ))
                                )
                            except TimeoutException:
                                logger.warning(f"Product cards not found for {subcat_name}")
                                continue

                            # Scroll to load all products
                            self.ps.scroll_all(driver, actual_card_sel)


                            # Parse
                            records = self.psr.parse(driver, city, zone, pincode, cat_name, subcat_name)

                            if records:
                                logger.info(f"Parsed {len(records)} records for {subcat_name}")
                                all_records.extend(records)
                                # Dual-write: CSV + Kafka
                                df_inc = pd.DataFrame(records, columns=PRODUCT_SCHEMA)
                                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                                out = RAW_DIR / f"bigbasket_{city}_{pincode}_{ts}.csv"
                                df_inc.to_csv(out, index=False, encoding="utf-8-sig")
                                logger.info(f"Saved {len(records)} records to {out}")
                                self.kafka.publish_batch(records)
                            else:
                                logger.warning(f"No records for {subcat_name} at {pincode}")

                            time.sleep(random.uniform(3, 6))
                        except Exception as e:
                            logger.error(f"Error scraping {city}|{pincode}|{subcat_name}: {e}\n{traceback.format_exc()}")
                            continue

        if driver:
            self.bm.quit(driver)
        self.kafka.close()

        df = pd.DataFrame(all_records, columns=PRODUCT_SCHEMA)
        logger.info(f"Pipeline finished in {time.monotonic() - t0:.1f}s. Scraped {len(df)} total items.")
        return df


# In[10]:

# ═══════════════════════════════════════════════════════════════
#  CELL 10 · RUN EXECUTOR (Multi-City)
# ═══════════════════════════════════════════════════════════════
def run_full_scrape():
    """Run the pipeline across all cities defined in CITY_CONFIG."""
    pipe = BBPipeline(headless=False, max_prod=200) # Full scrape per category
    logger.info("Starting FULL multi-city BigBasket scrape...")
    df = pipe.run(city_config=CITY_CONFIG)
    
    print(f"\n{'='*60}")
    print(f"Records scraped: {len(df)}")
    if len(df) > 0:
        print(f"Columns: {list(df.columns)}")
        print(f"\nSample records:")
        # Print a clean sample of the extracted fields
        cols_to_show = ["city", "pincode", "product_name", "variant", "current_price", "mrp", "discount_perc", "in_stock", "rating", "pack_size"]
        # Only show columns that actually exist
        cols_to_show = [c for c in cols_to_show if c in df.columns]
        print(df[cols_to_show].head(15).to_string())
    return df

# Start the full scrape
if __name__ == "__main__":
    df = run_full_scrape()



# In[11]:

# ═══════════════════════════════════════════════════════════════
#  CELL 11 · DIAGNOSTIC: DOM Inspector (find correct selectors)
# ═══════════════════════════════════════════════════════════════
def diag_inspect_dom():
    """
    Opens BigBasket Fruits & Vegetables page and dumps DOM snippets.
    Use this to find/validate correct CSS selectors for product cards.
    """
    bm = BrowserManager(headless=False)
    driver = bm.create_driver()

    lm = BBLocationManager(BIGBASKET_CFG)
    lm.set_location(driver, "700001")

    driver.get("https://www.bigbasket.com/cl/fruits-vegetables/?nc=nb")
    time.sleep(8)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Try different card selectors
    strategies = {
        "SKUDeck Div": soup.select("div[class*='SKUDeck___StyledDiv']"),
        "sc-kIRgvC Li": soup.select("li[class*='sc-kIRgvC']"),
        "data-qa='product'": soup.select("div[data-qa='product']"),
        "li.SKUDeck": soup.select("li.SKUDeck"),
        "div.SKUDeck": soup.select("div.SKUDeck"),
    }

    for name, cards in strategies.items():
        print(f"\n{'='*60}")
        print(f"Selector: {name} → {len(cards)} cards found")
        if cards:
            print(f"First card HTML (truncated):")
            print(str(cards[0])[:500])

    # Save full page for analysis
    debug_path = DEBUG_DIR / "bb_dom_dump.html"
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print(f"\nFull DOM saved to {debug_path}")

    bm.quit(driver)

# Uncomment to run DOM inspector:
diag_inspect_dom()


# In[12]:

# ═══════════════════════════════════════════════════════════════
#  CELL 12 · FULL RUN
# ═══════════════════════════════════════════════════════════════
def full_run():
    """Production run across all cities and categories."""
    pipe = BBPipeline(headless=True, max_prod=200)
    df = pipe.run()
    print(f"\nTotal records: {len(df)}")
    print(f"By city: {df.groupby('city').size().to_dict()}")
    print(f"By category: {df.groupby('category').size().to_dict()}")
    return df

# Uncomment to run full scrape:
# df = full_run()
