#!/usr/bin/env python
# coding: utf-8

# # 🧠 Real-Time Market Intelligence Engine — R&D Notebook
# ## 🛒 JioMart Full Catalog Scraper (Selenium + Dynamic Discovery)
# 
# This notebook demonstrates the robust DOM parsing architecture with **Dynamic Category Discovery** to scrape the full JioMart catalog across representative geographic boundaries without hitting protected APIs. 
# 
# **Core Classes**:
# - `BrowserManager`: Handles Headless Chrome, Anti-Detection patches, mouse jitter.
# - `LocationManager`: Injects Pincode via `localStorage` or UI interaction.
# - `CategoryManager`: Crawls L1 -> L2/L3 catalog tree automatically.
# - `PageScroller`: Scrolls incrementally with jitter to lazy-load elements.
# - `ProductParser`: Uses exact DOM element matching for JioMart `gtmEvents` + fallback parsing.
# - `MarketIntelligencePipeline`: Coordinates the scrape, manages limits (`MAX_CATEGORIES`), and outputs DataFrame & CSV.
# 
# > Note: The production-ready versions of these classes are available in `services/scrapers/jiomart_selenium.py`.

# In[1]:


import os, re, time, json, random, logging
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
METRICS_DIR  = DATA_DIR / "metrics"
LOGS_DIR     = PROJECT_ROOT / "logs"

for d in [RAW_DIR, DEBUG_DIR, METRICS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    log = logging.getLogger("jiomart_notebook")
    if log.handlers: return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(thread)-12s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOGS_DIR / "scraper_rnd.log", encoding="utf-8")
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

JIOMART_CFG = {
    "source": "jiomart",
    "home_url": "https://www.jiomart.com/",
    # Search page that shows the left-sidebar with all L2 categories + subcategories
    "grocery_search_url": "https://www.jiomart.com/search?q=Groceries&searchtype=schedule",
    "product_card": "li.ais-InfiniteHits-item",
    "product_wrapper": "a.plp-card-wrapper",
    "gtm_events": "div.gtmEvents",
    "price_sel": ".plp-card-details-price .jm-heading-xxs",
    "mrp_sel": ".plp-card-details-price .line-through",
    "discount_sel": ".jm-badge",
    "variant_sel": ".variant_value",
    "img_container": ".plp-card-image",
    "show_more_btn": ".show_more button",
    # Selectors for sidebar categories/subcategories
    "l2_category_item": "#categories_filter .ais-HierarchicalMenu-item--child .ais-HierarchicalMenu-label",
    "subcategory_item": "#attributes\\.category_level_4_filter .ais-RefinementList-item",
    "subcategory_label": ".ais-RefinementList-labelText",
    "subcategory_checkbox": ".ais-refinement-list--checkbox",
    "show_more_subcats": ".filters-box .show_more button",
}

PRODUCT_SCHEMA = [
    "source", "city", "zone", "pincode", "category", "subcategory",
    "product_name", "brand", "variant", "current_price", "mrp",
    "discount_percent", "in_stock", "rating", "pack_size",
    "image_url", "scraped_at"
]

print("✅ Config ready.")


# In[3]:


# ═══════════════════════════════════════════════════════════════
#  CELL 3 · BROWSER & LOCATION MANAGERS
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


class LocationManager:
    """Sets JioMart delivery location using the area-search Google Places UI.
    
    FIX v2: The confirm button uses Angular component classes, not generic CSS.
    Selector: button[aria-label='button Confirm Location'] (j-button with that aria-label).
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.pincode_to_area = {
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
            "560105": "Electronic City Phase 2, Bangalore",
            # Pune
            "411001": "Camp, Pune",
            "411004": "Deccan, Pune",
            "411014": "Viman Nagar, Pune",
            "411057": "Hinjewadi, Pune",
            "412105": "Talegaon, Pune",
            "412308": "Loni Kalbhor, Pune",
        }

    def set_location(self, driver: webdriver.Chrome, pincode: str) -> bool:
        logger.info(f"Setting location for {pincode}")
        area_query = self.pincode_to_area.get(pincode, pincode)

        driver.get("https://www.jiomart.com/")
        time.sleep(random.uniform(4, 6))

        # Step 1: Open the location modal
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "select_location_popup"))
            )
            btn.click()
            logger.info("Clicked 'Select Location Manually'")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Modal trigger not found: {e}")
            driver.get("https://www.jiomart.com/customer/guestmap")
            time.sleep(3)

        # Step 2: Type area into Google Places search input
        try:
            search_input = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input#searchin[placeholder*='area']"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", search_input)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", search_input)
            time.sleep(0.5)
            search_input.clear()
            for ch in area_query:
                search_input.send_keys(ch)
                time.sleep(random.uniform(0.05, 0.12))
            logger.info(f"Typed area: {area_query}")
            time.sleep(2.5)
        except Exception as e:
            logger.warning(f"Could not type in search box: {e}")
            return False

        # Step 3: Click first autocomplete suggestion
        try:
            first_suggestion = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".pac-item:first-child"))
            )
            first_suggestion.click()
            logger.info(f"Clicked first autocomplete result for: {area_query}")
            time.sleep(random.uniform(3, 5))
        except Exception as e:
            logger.warning(f"No autocomplete suggestion found, trying RETURN: {e}")
            try:
                search_input.send_keys(Keys.ARROW_DOWN)
                time.sleep(0.5)
                search_input.send_keys(Keys.RETURN)
                time.sleep(random.uniform(3, 5))
            except:
                return False

        # Step 4: FIX — Click the Angular 'Confirm Location' button
        # The button has aria-label='button Confirm Location' and class j-button
        confirm_selectors = [
            "button[aria-label='button Confirm Location']",   # Angular aria-label (PRIMARY)
            "button.j-button[name='jds-button']",             # Angular j-button fallback
            ".j-button.primary",                              # Generic Angular primary btn
            "button[class*='confirm']",                       # Legacy fallback
            ".ep-pincode-btn",
        ]
        confirmed = False
        for sel in confirm_selectors:
            try:
                confirm_btn = WebDriverWait(driver, 6).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
                )
                driver.execute_script("arguments[0].click();", confirm_btn)
                logger.info(f"Clicked confirm button via selector: {sel}")
                time.sleep(random.uniform(3, 5))
                confirmed = True
                break
            except:
                continue

        if not confirmed:
            logger.info("No confirm button found — location may be set already.")

        # Step 5: Verify
        try:
            delivery_text = driver.find_element(
                By.CSS_SELECTOR, ".delivery-pincode, [class*='delivery'], .location-text"
            ).text
            logger.info(f"Location set. Banner shows: {delivery_text}")
        except:
            logger.info(f"Location set for {pincode} (banner check skipped).")

        return True


# In[4]:


# ═══════════════════════════════════════════════════════════════
#  CELL 4 · CATEGORY DISCOVERY (Search Sidebar v3)
# ═══════════════════════════════════════════════════════════════
class CategoryManager:
    """Crawls L2 categories dynamically using the global Search method and the left filter block."""
    
    def __init__(self, cfg: dict, max_categories: int=5, max_subcategories: int = 15):
        self.cfg = cfg
        self.max_categories = max_categories
        self.max_subcategories = max_subcategories
        
        # User defined targets
        self.target_l1_name = "Groceries"

    def discover_catalog(self, driver) -> list:
        import logging
        import time, random
        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import urllib.parse
        
        logger = logging.getLogger("jiomart_notebook")
        catalog = []
        
        target_l2_names = ['Dairy & Bakery', 'Fruits & Vegetables', 'Snacks & Branded Foods']
        logger.info(f"Using Search Discovery for {self.target_l1_name} across targets: {target_l2_names}")
        
        for l2 in target_l2_names:
            try:
                # 1. Direct Navigation to Search Results for target L2 category
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
                
                # Remove backdrops that intercept clicks
                driver.execute_script("document.querySelectorAll('.location-backdrop').forEach(el => el.remove());")
                
                # 3. Expand Sub Categories via +More button
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
                    
                # 4. Parse Subcategories
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

    def discover_catalog_cached(self, driver, cache_max_age_hours: int = 24) -> list:
        """Load catalog from cache if fresh, otherwise discover and save."""
        import json, logging
        from datetime import datetime, timedelta
        logger = logging.getLogger("jiomart_notebook")
        
        cache_dir = RAW_DIR.parent / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / "catalog_cache.json"
        
        # Check if cache exists and is fresh
        if cache_file.exists():
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                cached_at = datetime.fromisoformat(cached["timestamp"])
                if datetime.utcnow() - cached_at < timedelta(hours=cache_max_age_hours):
                    catalog = cached["catalog"]
                    logger.info(f"Loaded {len(catalog)} catalog nodes from cache (age: {datetime.utcnow() - cached_at}).")
                    return catalog
                else:
                    logger.info("Cache expired, re-discovering catalog.")
            except Exception as e:
                logger.warning(f"Cache read failed: {e}")
        
        # Discover fresh
        catalog = self.discover_catalog(driver)
        
        # Save to cache
        if catalog:
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump({"timestamp": datetime.utcnow().isoformat(), "catalog": catalog}, f, indent=2)
                logger.info(f"Saved {len(catalog)} catalog nodes to cache: {cache_file}")
            except Exception as e:
                logger.warning(f"Cache write failed: {e}")
        
        return catalog



# In[5]:


# ═══════════════════════════════════════════════════════════════
#  CELL 5 · SCROLLER & PARSER
# ═══════════════════════════════════════════════════════════════
class PageScroller:
    def __init__(self, max_scrolls: int = 60):
        self.max_scrolls = max_scrolls

    def scroll_all(self, driver: webdriver.Chrome, card_selector: str) -> int:
        last_count = len(driver.find_elements(By.CSS_SELECTOR, card_selector))
        stable_iters = 0
        steps = 0

        while steps < self.max_scrolls:
            driver.execute_script("window.scrollBy(0, window.innerHeight);")
            BrowserManager.jitter_mouse(driver)
            time.sleep(random.uniform(1.5, 3.0))

            try:
                elem = driver.find_element(By.CSS_SELECTOR, ".show_more button")
                if elem.is_displayed():
                    driver.execute_script("arguments[0].click();", elem)
                    time.sleep(2)
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


class ProductParser:
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

    def parse(self, driver: webdriver.Chrome, city: str, zone: str, pincode: str,
              cat_name: str, subcat_name: str) -> List[dict]:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = soup.select(self.cfg["product_card"])
        ts = datetime.utcnow().isoformat() + "Z"

        records = []
        for item in items[:self.max_products]:
            try:
                gtm = item.select_one(self.cfg["gtm_events"])
                wrapper = item.select_one(self.cfg["product_wrapper"])

                name, brand, price = None, None, None
                mrp, img_url, variant, discount, pack_size = None, None, None, None, None

                if gtm:
                    name = gtm.get("data-name")
                    brand = gtm.get("data-manu")
                    price = self._clean_price(gtm.get("data-price"))
                    img_url = gtm.get("data-image")

                if not name and wrapper: name = wrapper.get("title", "")

                price_dom = item.select_one(self.cfg["price_sel"])
                if price_dom and not price: price = self._clean_price(price_dom.text)

                mrp_dom = item.select_one(self.cfg["mrp_sel"])
                if mrp_dom: mrp = self._clean_price(mrp_dom.text)

                disc_dom = item.select_one(self.cfg["discount_sel"])
                if disc_dom:
                    discount = self._clean_perc(disc_dom.text)
                elif price and mrp and mrp > 0:
                    discount = round((mrp - price) / mrp * 100, 2)

                var_dom = item.select_one(self.cfg["variant_sel"])
                if var_dom:
                    variant = var_dom.text.strip()
                    pack_size = variant

                if not img_url:
                    img_dom = item.select_one(self.cfg["img_container"] + " img")
                    if img_dom: img_url = img_dom.get("data-src") or img_dom.get("src")

                in_stock = True
                if item.select_one(".out-of-stock") or "out of stock" in str(item).lower():
                    in_stock = False

                if not name: continue

                records.append({
                    "source": self.source, "city": city, "zone": zone, "pincode": pincode,
                    "category": cat_name, "subcategory": subcat_name, "product_name": name,
                    "brand": brand, "variant": variant, "current_price": price, "mrp": mrp,
                    "discount_percent": discount, "in_stock": in_stock, "rating": None,
                    "pack_size": pack_size, "image_url": img_url, "scraped_at": ts,
                })
            except Exception as e:
                logger.error(f"Error parsing card: {e}")

        seen, deduped = set(), []
        for r in records:
            key = (r['product_name'], r['pincode'])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        return deduped


# In[6]:


# ═══════════════════════════════════════════════════════════════
#  CELL 5b · KAFKA PRODUCER (optional — enabled via KAFKA_BROKER env)
# ═══════════════════════════════════════════════════════════════
class KafkaProducerWrapper:
    """Publishes scraped records to Kafka in the unified schema.
    
    Activation: Set env var KAFKA_BROKER (e.g. 'localhost:29092').
    If not set, all publish calls are silent no-ops.
    """
    
    # Map scraper field names → Kafka schema field names
    FIELD_MAP = {
        "category":         "category_l2",
        "subcategory":      "category_l3",
        "current_price":    "selling_price",
        "discount_percent": "discount_pct",
    }
    
    def __init__(self, source: str = "jiomart"):
        self.source = source
        self.topic = f"raw.{source}"
        self.producer = None
        self._delivery_errors = 0
        
        broker = os.environ.get("KAFKA_BROKER")
        if broker:
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
                logger.info(f"Kafka producer connected to {broker} → topic: {self.topic}")
            except Exception as e:
                logger.warning(f"Kafka producer init failed: {e}. Publishing disabled.")
                self.producer = None
        else:
            logger.info("KAFKA_BROKER not set — Kafka publishing disabled.")
    
    @property
    def enabled(self) -> bool:
        return self.producer is not None
    
    def _delivery_callback(self, err, msg):
        if err:
            self._delivery_errors += 1
            logger.warning(f"Kafka delivery failed: {err}")
    
    def _to_kafka_schema(self, record: dict) -> dict:
        """Convert scraper record to unified Kafka message schema."""
        msg = {}
        for old_key, val in record.items():
            new_key = self.FIELD_MAP.get(old_key, old_key)
            msg[new_key] = val
        msg["event_type"] = "product_price"
        msg.setdefault("product_url", None)
        return msg
    
    def publish_batch(self, records: list):
        """Publish a batch of scraper records to Kafka. No-op if disabled."""
        if not self.enabled or not records:
            return
        
        import json
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
                logger.warning("Kafka producer queue full, flushing...")
                self.producer.flush(timeout=5)
                self.producer.produce(
                    topic=self.topic,
                    key=record.get("pincode", "").encode("utf-8"),
                    value=json.dumps(msg, default=str).encode("utf-8"),
                    callback=self._delivery_callback,
                )
        
        self.producer.flush(timeout=10)
        logger.info(f"Published {len(records)} records to Kafka topic {self.topic}")
    
    def close(self):
        if self.producer:
            self.producer.flush(timeout=10)
            logger.info(f"Kafka producer closed. Delivery errors: {self._delivery_errors}")


# ═══════════════════════════════════════════════════════════════
#  CELL 6 · ORCHESTRATION PIPELINE 
# ═══════════════════════════════════════════════════════════════
class MarketIntelligencePipeline:
    """Orchestrates targeted scraping across geography and discovering catalog."""
    
    def __init__(self, headless: bool = True, max_cat: int = 1, max_sub: int = 15, max_prod: int = 200):
        self.headless = headless
        self.bm = BrowserManager(headless=headless)
        self.lm = LocationManager(JIOMART_CFG)
        self.cm = CategoryManager(JIOMART_CFG, max_categories=max_cat, max_subcategories=max_sub)
        self.ps = PageScroller()
        self.psr = ProductParser(JIOMART_CFG, max_products=max_prod)
        self.kafka = KafkaProducerWrapper(source=JIOMART_CFG["source"])

    def _apply_l4_filter(self, driver, l4_filter_value, logger):
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time
        try:
            logger.info(f"Applying L4 filter: {l4_filter_value}")
            val_escaped = l4_filter_value.replace("'", "\'")
            xpath = f"//input[@name='attributes.category_level_4' and @value='{val_escaped}']"
            
            # Check if it's already visible in the sidebar without opening the modal
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
                    more_btn = driver.find_element(By.CSS_SELECTOR, "div[data-attr='attributes.category_level_4'] .show_more button")
                    if "none" not in more_btn.find_element(By.XPATH, "..").get_attribute("style"):
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_btn)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", more_btn)
                        time.sleep(2)
                        
                        # Wait for modal
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "popup_filters")))
                        
                        # Find checkbox inside modal
                        chk_input = driver.find_element(By.XPATH, f"//ul[@id='popup_filters']{xpath}")
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", chk_input)
                        time.sleep(1)
                        driver.execute_script("arguments[0].click();", chk_input)
                        
                        # Click Apply
                        try:
                            apply_btn = driver.find_element(By.ID, "filter_popup_apply")
                            driver.execute_script("arguments[0].click();", apply_btn)
                        except Exception as apply_btn_err:
                            logger.warning(f"Failed clicking modal 'Apply' button: {apply_btn_err}")
                            
                        clicked = True
                except Exception as modal_err:
                    logger.warning(f"Failed opening modal to click filter: {modal_err}")
            
            if not clicked:
                logger.warning(f"Filter element '{l4_filter_value}' is nowhere to be found in DOM.")
                
            time.sleep(5)
            logger.info("Filter applied successfully.")
        except Exception as e:
            logger.warning(f"Could not click L4 filter '{l4_filter_value}': {e}")

    def run(self, city_config: dict, output_csv=None) -> pd.DataFrame:
        import time, random, logging, traceback
        from datetime import datetime
        import pandas as pd
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        logger = logging.getLogger("jiomart_notebook")
        all_records = []
        t0 = time.monotonic()
        
        driver = None
        pincodes_scraped_since_restart = 0
        
        # ── Phase 1: Discover catalog ONCE globally ──────────────
        driver = self.bm.create_driver()
        first_city = list(city_config.keys())[0]
        first_pincode = list(city_config[first_city].values())[0][0]
        self.lm.set_location(driver, first_pincode)
        catalog = self.cm.discover_catalog_cached(driver)
        self.bm.quit(driver)
        driver = None
        
        logger.info(f"Global catalog: {len(catalog)} nodes. Starting multi-city scrape.")
        if not catalog:
            logger.error("Catalog discovery failed globally. Aborting.")
            return pd.DataFrame(columns=PRODUCT_SCHEMA)
        
        # ── Phase 2: Scrape all cities using cached catalog ──────
        for city, zones in city_config.items():
            logger.info(f"=== Starting city: {city} ===")
            
            for zone, pincodes in zones.items():
                for pincode in pincodes:
                    if not driver or pincodes_scraped_since_restart >= 2:
                        if driver:
                            logger.info("Anti-bot: Restarting browser session.")
                            self.bm.quit(driver)
                        driver = self.bm.create_driver()
                        pincodes_scraped_since_restart = 0
                    
                    self.lm.set_location(driver, pincode)
                    pincodes_scraped_since_restart += 1
                    
                    for cat_node in catalog:
                        cat_name = cat_node["category"]
                        subcat_name = cat_node["subcategory"]
                        url = cat_node["url"]
                        l4_filter_value = cat_node.get("l4_filter_value")
                        
                        logger.info(f"[{city}|{zone}|{pincode}] Scraping: {cat_name} -> {subcat_name}")
                        try:
                            logger.info(f"Navigating to {url}")
                            driver.get(url)
                            time.sleep(random.uniform(4, 7))
                            
                            driver.execute_script("document.querySelectorAll('.location-backdrop').forEach(el => el.remove());")
                            
                            try:
                                logger.info(f"Expanding category: {cat_name}")
                                grocery_lbl_xpath = f"//div[@data-attr='categories']//span[text()='{cat_name}']"
                                grocery_lbl = WebDriverWait(driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, grocery_lbl_xpath))
                                )
                                driver.execute_script("arguments[0].click();", grocery_lbl)
                                time.sleep(4)
                            except Exception as e:
                                logger.warning(f"Could not expand main category '{cat_name}' before filtering: {e}")
                                
                            driver.execute_script("document.querySelectorAll('.location-backdrop').forEach(el => el.remove());")

                            if l4_filter_value:
                                self._apply_l4_filter(driver, l4_filter_value, logger)
                            
                            logger.info("Scrolling page...")
                            self.ps.scroll_all(driver, JIOMART_CFG["product_card"])
                            
                            logger.info("Parsing products...")
                            records = self.psr.parse(driver, city, zone, pincode, cat_name, subcat_name)
                            
                            if records:
                                logger.info(f"Parsed {len(records)} records for {subcat_name}")
                                all_records.extend(records)
                                # Dual-write: CSV (always) + Kafka (if enabled)
                                df_inc = pd.DataFrame(records, columns=PRODUCT_SCHEMA)
                                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                                out = RAW_DIR / f"jiomart_{city}_{pincode}_{ts}.csv"
                                df_inc.to_csv(out, index=False, encoding="utf-8-sig")
                                logger.info(f"Saved {len(records)} records to {out}")
                                self.kafka.publish_batch(records)
                            else:
                                logger.warning(f"No records found for {subcat_name} at pincode {pincode}.")
                            
                            time.sleep(random.uniform(4, 7))
                            logger.info(f"Successfully finished cycle for {subcat_name}")
                        except Exception as e:
                            logger.error(f"Error scraping {city}|{pincode}|{subcat_name}: {e}\n{traceback.format_exc()}")
                            # Do not let one subcategory failure break the whole pincode
                            continue

        if driver:
            self.bm.quit(driver)
        self.kafka.close()

        df = pd.DataFrame(all_records, columns=PRODUCT_SCHEMA)
        logger.info(f"Pipeline finished in {time.monotonic() - t0:.2f}s. Scraped {len(df)} total items.")
        return df



# In[7]:


# ═══════════════════════════════════════════════════════════════
#  CELL 7a · CATALOG-ONLY RUN (debug / preview categories)
#  Run this first to verify category discovery works before full scrape.
# ═══════════════════════════════════════════════════════════════
bm = BrowserManager(headless=True)
lm = LocationManager(JIOMART_CFG)
cm = CategoryManager(JIOMART_CFG)

driver = bm.create_driver()
lm.set_location(driver, "700001")
catalog = cm.discover_catalog(driver)
bm.quit(driver)

cat_df = pd.DataFrame(catalog)
print(f"\n✅ Discovered {len(cat_df)} subcategories across {cat_df['category'].nunique()} L2 categories.")
print(cat_df)

# Save catalog to CSV for inspection
cat_csv = RAW_DIR / f"jiomart_catalog_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
cat_df.to_csv(cat_csv, index=False, encoding="utf-8-sig")
print(f"Catalog saved to: {cat_csv}")


# In[ ]:


# ═══════════════════════════════════════════════════════════════
#  CELL 7b · FULL PIPELINE RUN
#  Remove max_cat / max_sub limits for a complete scrape.
# ═══════════════════════════════════════════════════════════════
pipeline = MarketIntelligencePipeline(
    headless=True,
    max_cat=99,    # set to 2 for quick test
    max_sub=999,   # set to 3 for quick test
    max_prod=200
)
df = pipeline.run(CITY_CONFIG)

print(df[['category', 'subcategory', 'product_name', 'current_price', 'in_stock']].head(20))
print(f"\nTotal rows: {len(df)}")
print(f"Categories: {df['category'].nunique()}")
print(f"Subcategories: {df['subcategory'].nunique()}")
print(f"In stock: {df['in_stock'].sum()} / {len(df)}")


# In[8]:


# DEBUG CELL — run this standalone, paste full output back
import time, random
from pathlib import Path

bm = BrowserManager(headless=True)
lm = LocationManager(JIOMART_CFG)
driver = bm.create_driver()
lm.set_location(driver, "700001")

driver.get("https://www.jiomart.com/search?q=Groceries&searchtype=schedule")
time.sleep(8)

# 1. What URL did we land on?
print("CURRENT URL:", driver.current_url)

# 2. Does #categories_filter exist at all?
cats = driver.find_elements("css selector", "#categories_filter")
print("categories_filter found:", len(cats))

# 3. Try every plausible selector variant
selectors = [
    "#categories_filter .ais-HierarchicalMenu-list--child .ais-HierarchicalMenu-link",
    "#categories_filter .ais-HierarchicalMenu-item--child .ais-HierarchicalMenu-link",
    "#categories_filter .ais-HierarchicalMenu-link",
    ".ais-HierarchicalMenu-link",
    ".ais-HierarchicalMenu-label",
]
for sel in selectors:
    els = driver.find_elements("css selector", sel)
    print(f"  [{len(els):>3}] {sel}")

# 4. Save the page so we can inspect it
debug_path = Path("E:/cv projects/real_time-market-intelligence/data/debug/search_page_debug.html")
debug_path.parent.mkdir(parents=True, exist_ok=True)
with open(debug_path, "w", encoding="utf-8") as f:
    f.write(driver.page_source)
print("HTML saved to:", debug_path)

bm.quit(driver)

