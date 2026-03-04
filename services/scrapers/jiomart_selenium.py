import os
import re
import time
import json
import random
import logging
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
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
DEBUG_DIR    = DATA_DIR / "debug"
METRICS_DIR  = DATA_DIR / "metrics"
LOGS_DIR     = PROJECT_ROOT / "logs"

for d in [RAW_DIR, DEBUG_DIR, METRICS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    log = logging.getLogger("jiomart_selenium")
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(module)-15s | %(message)s",
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

# ── Config ────────────────────────────────────────────────────
CHROME_VERSION = "145.0.7632.76"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

JIOMART_CFG = {
    "source": "jiomart",
    "home_url": "https://www.jiomart.com/",
    
    # Selectors from instructions
    "product_card": "li.ais-InfiniteHits-item",
    "product_wrapper": "a.plp-card-wrapper",
    "gtm_events": "div.gtmEvents",
    
    "price_sel": ".plp-card-details-price .jm-heading-xxs",
    "mrp_sel": ".plp-card-details-price .line-through",
    "discount_sel": ".jm-badge",
    "variant_sel": ".variant_value",
    "img_container": ".plp-card-image",
    
    "show_more_btn": ".show_more button",
}

PRODUCT_SCHEMA = [
    "source", "city", "zone", "pincode", "category", "subcategory",
    "product_name", "brand", "variant", "current_price", "mrp", 
    "discount_percent", "in_stock", "rating", "pack_size", 
    "image_url", "scraped_at"
]


# ═══════════════════════════════════════════════════════════════
#  Class 1 · BrowserManager
# ═══════════════════════════════════════════════════════════════
class BrowserManager:
    """Manages Chrome creation, anti-detection, and warm-up."""
    
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
        if self.headless:
            opts.add_argument("--headless=new")
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
                # Anti-detection CDP
                driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": self.user_agent})
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                """})
                logger.info(f"Browser built successfully (attempt {attempt+1}). Headless={self.headless}")
                return driver
            except Exception as e:
                logger.error(f"Failed to init browser attempt {attempt+1}: {e}")
                time.sleep(2)
        
        raise WebDriverException("Could not initialize browser after 2 attempts.")

    @staticmethod
    def jitter_mouse(driver: webdriver.Chrome):
        """Simulate micro mouse movements in JS."""
        try:
            driver.execute_script(f"""
                var evt = new MouseEvent('mousemove', {{
                    clientX: {random.randint(100, 800)},
                    clientY: {random.randint(100, 800)}
                }});
                document.dispatchEvent(evt);
            """)
        except:
            pass

    @staticmethod
    def quit(driver: webdriver.Chrome):
        try: driver.quit()
        except: pass


# ═══════════════════════════════════════════════════════════════
#  Class 2 · LocationManager
# ═══════════════════════════════════════════════════════════════
class LocationManager:
    """Manages setting pincode in JioMart."""
    
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def set_location(self, driver: webdriver.Chrome, pincode: str) -> bool:
        """Sets location. Tries local_storage + refresh, or UI."""
        logger.info(f"Setting location context for pincode {pincode}")
        
        driver.get(self.cfg["home_url"])
        time.sleep(random.uniform(4, 8))
        
        # Attempt 1: UI click
        trigger_sels = [
            "[class*='location']", "[class*='delivery']", "[aria-label*='location']", ".location-widget"
        ]
        
        for attempt in range(2):
            clicked = False
            for sel in trigger_sels:
                try:
                    el = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                    el.click()
                    clicked = True
                    time.sleep(1.5)
                    break
                except: continue
            
            if clicked:
                try:
                    inp = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "input[placeholder*='pincode'], input[placeholder*='Pincode'], input[id*='pin']"))
                    )
                    inp.clear()
                    for ch in pincode:
                        inp.send_keys(ch)
                        time.sleep(random.uniform(0.05, 0.15))
                    inp.send_keys(Keys.RETURN)
                    time.sleep(random.uniform(3, 6))
                    logger.info(f"Pincode {pincode} set via UI.")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to type in UI pincode modal: {e}")
            
            # Attempt 2: Local Storage Inject
            logger.info("Attempting localStorage injection for pincode.")
            try:
                driver.execute_script(f"localStorage.setItem('userPinCode', '{pincode}');")
                driver.refresh()
                time.sleep(random.uniform(3, 6))
                logger.info(f"Pincode {pincode} injected to localStorage.")
                return True
            except Exception as e:
                logger.warning(f"Local storage inject failed: {e}")
                
        logger.warning(f"Exhausted 2 attempts to set pincode {pincode}.")
        return False


# ═══════════════════════════════════════════════════════════════
#  Class 3 · CategoryManager (TARGETED)
# ═══════════════════════════════════════════════════════════════
class CategoryManager:
    """Crawls specific 'Biscuits, Drinks & Packaged Foods' category."""
    
    def __init__(self, cfg: dict, max_subcategories: int = 15):
        self.cfg = cfg
        self.max_subcategories = max_subcategories
        
        self.target_l1_name = "Biscuits, Drinks & Packaged Foods"
        self.target_l1_url = "https://www.jiomart.com/c/groceries/biscuits-drinks-packaged-foods/28996"

    def get_l2_categories_dynamically(self, driver) -> list:
        """Dynamically parses the L2 subcategories from the specific target URL."""
        logger.info(f"Targeting {self.target_l1_name} to dynamically fetch L2 categories from {self.target_l1_url}")
        
        driver.get(self.target_l1_url)
        
        # Explicitly wait for the exact requested L2 container to load in the DOM
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.l2_category_container div.l2_category_cat"))
            )
        except Exception as e:
            logger.warning(f"Timeout waiting for L2 container to load: {e}")
            
        # Give a small delay to ensure any internal images or states finalize
        time.sleep(random.uniform(2, 4))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        subcats = []
        
        # The exact L2 category container from JioMart
        containers = soup.select("div.l2_category_container div.l2_category_cat")
        if containers:
            for cont in containers:
                link = cont.select_one("a.l2_content_link, a")
                if link and "/c/" in link.get("href", ""):
                    href = link["href"]
                    full_url = href if href.startswith("http") else f"https://www.jiomart.com{href}"
                    
                    name_span = cont.select_one(".l2_category_cat_text")
                    name = name_span.text.strip() if name_span else (link.get("title") or link.text.strip())
                    
                    subcats.append({"name": name, "url": full_url})
        else:
            logger.warning("Could not find dynamic L2 container div.l2_category_container. Subcategories not loaded.")
            
        logger.info(f"Discovered {len(subcats)} L2 subcategories dynamically.")
        return subcats[:self.max_subcategories]

    def discover_catalog(self, driver: webdriver.Chrome) -> List[Dict]:
        """Returns a flat list of dicts mapped to the explicitly targeted L2s and their L4 subcategories."""
        catalog = []
        subs = self.get_l2_categories_dynamically(driver)
            
        for sub in subs:
            logger.info(f"Discovering L4 filters for {sub['name']} at {sub['url']}")
            driver.get(sub["url"])
            time.sleep(random.uniform(4, 7))
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            l4_container = soup.select_one("#attributes\\.category_level_4_filter, div[id*='category_level_'] .ais-RefinementList")
            
            l4_filters = []
            if l4_container:
                items = l4_container.select("li.ais-RefinementList-item label input")
                for item in items:
                    val = item.get("value")
                    name_el = item.find_next_sibling("span")
                    name = name_el.text.strip() if name_el else val
                    if val:
                        l4_filters.append({"name": name, "value": val})
            
            if l4_filters:
                logger.info(f"Discovered {len(l4_filters)} L4 filters for {sub['name']}.")
                for l4 in l4_filters:
                    catalog.append({
                        "category": self.target_l1_name,
                        "subcategory": f"{sub['name']} -> {l4['name']}",
                        "url": sub["url"],
                        "l4_filter_value": l4["value"]
                    })
            else:
                logger.info(f"No L4 filters found for {sub['name']}, using L2 directly.")
                catalog.append({
                    "category": self.target_l1_name,
                    "subcategory": sub["name"],
                    "url": sub["url"],
                    "l4_filter_value": None
                })
                    
        return catalog


# ═══════════════════════════════════════════════════════════════
#  Class 4 · PageScroller
# ═══════════════════════════════════════════════════════════════
class PageScroller:
    """Scrolls down a page incrementally until no new content loads."""
    
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
        logger.info(f"Scrolled {steps} times. Found ~{last_count} elements.")
        return last_count


# ═══════════════════════════════════════════════════════════════
#  Class 5 · ProductParser
# ═══════════════════════════════════════════════════════════════
class ProductParser:
    """Parses JioMart product cards based on DOM."""
    
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

    def parse(self, driver: webdriver.Chrome, city: str, zone: str, pincode: str, cat_name: str, subcat_name: str) -> List[dict]:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = soup.select(self.cfg["product_card"])
        ts = datetime.utcnow().isoformat() + "Z"
        
        records = []
        
        for item in items[:self.max_products]:
            try:
                gtm = item.select_one(self.cfg["gtm_events"])
                wrapper = item.select_one(self.cfg["product_wrapper"])
                
                name, brand, price = None, None, None
                mrp, img_url, prod_url, variant, discount = None, None, None, None, None
                rating = None
                pack_size = None
                
                if gtm:
                    name = gtm.get("data-name")
                    brand = gtm.get("data-manu")
                    price = self._clean_price(gtm.get("data-price"))
                    img_url = gtm.get("data-image")
                
                if not name and wrapper: 
                    name = wrapper.get("title", "")
                
                if wrapper and wrapper.get("href"):
                    prod_url = "https://www.jiomart.com" + wrapper.get("href")
                
                price_dom = item.select_one(self.cfg["price_sel"])
                if price_dom and not price:
                    price = self._clean_price(price_dom.text)
                
                mrp_dom = item.select_one(self.cfg["mrp_sel"])
                if mrp_dom:
                    mrp = self._clean_price(mrp_dom.text)
                    
                disc_dom = item.select_one(self.cfg["discount_sel"])
                if disc_dom:
                    discount = self._clean_perc(disc_dom.text)
                elif price and mrp and mrp > 0:
                    discount = round((mrp - price) / mrp * 100, 2)
                    
                var_dom = item.select_one(self.cfg["variant_sel"])
                if var_dom:
                    variant = var_dom.text.strip()
                    pack_size = variant  # Treat variant as pack_size if distinct pack size element is missing
                    
                if not img_url:
                    img_dom = item.select_one(self.cfg["img_container"] + " img")
                    if img_dom:
                        img_url = img_dom.get("data-src") or img_dom.get("src")
                
                in_stock = True
                if item.select_one(".out-of-stock") or "out of stock" in str(item).lower():
                    in_stock = False
                    
                if not name:
                    continue
                
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
                    "discount_percent": discount,
                    "in_stock": in_stock,
                    "rating": rating, # To be added if site supports it
                    "pack_size": pack_size,
                    "image_url": img_url,
                    "scraped_at": ts,
                })
            except Exception as e:
                logger.error(f"Error parsing card: {e}")
                
        # Deduplicate on product_name since ids are mixed
        seen = set()
        deduped = []
        for r in records:
            key = (r['product_name'], r['pincode'])
            if key not in seen:
                seen.add(key)
                deduped.append(r)

        return deduped


# ═══════════════════════════════════════════════════════════════
#  Class 6 · MarketIntelligencePipeline
# ═══════════════════════════════════════════════════════════════
class MarketIntelligencePipeline:
    """Orchestrates targeted scraping across geography and discovering catalog."""
    
    def __init__(self, headless: bool = True, max_sub: int = 15, max_prod: int = 200):
        self.headless = headless
        self.bm = BrowserManager(headless=headless)
        self.lm = LocationManager(JIOMART_CFG)
        self.cm = CategoryManager(JIOMART_CFG, max_subcategories=max_sub)
        self.ps = PageScroller()
        self.psr = ProductParser(JIOMART_CFG, max_products=max_prod)

    def run(self, city_config: dict) -> pd.DataFrame:
        all_records = []
        t0 = time.monotonic()
        
        driver = None
        pincodes_scraped_since_restart = 0
        
        for city, zones in city_config.items():
            
            # --- Per-City Catalog Discovery ---
            if not driver:
                driver = self.bm.create_driver()
            
            first_pincode = list(zones.values())[0][0]
            self.lm.set_location(driver, first_pincode)
            catalog = self.cm.discover_catalog(driver)
            
            logger.info(f"Discovered {len(catalog)} subcategories for {city}.")
            
            for zone, pincodes in zones.items():
                for pincode in pincodes:
                    # Anti-bot constraint: restart driver every 2 pincodes
                    if pincodes_scraped_since_restart >= 2:
                        logger.info("Anti-bot: Restarting browser session.")
                        self.bm.quit(driver)
                        driver = self.bm.create_driver()
                        pincodes_scraped_since_restart = 0
                    
                    self.lm.set_location(driver, pincode)
                    pincodes_scraped_since_restart += 1
                    
                    for cat_node in catalog:
                        cat_name = cat_node["category"]
                        subcat_name = cat_node["subcategory"]
                        l4_filter_value = cat_node.get("l4_filter_value")
                        
                        logger.info(f"[{city}|{zone}|{pincode}] Scraping: {cat_name} -> {subcat_name}")
                        try:
                            # 1. Nav
                            driver.get(url)
                            time.sleep(random.uniform(3, 6))
                            
                            # Click filter if L4
                            if l4_filter_value:
                                try:
                                    logger.info(f"Applying L4 filter: {l4_filter_value}")
                                    # Escape quotes if necessary, though values usually don't have single quotes here
                                    xpath = f"//input[@type='checkbox' and @value=\"{l4_filter_value}\"]/parent::label"
                                    # Wait until the filter is present
                                    label_el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                                    driver.execute_script("arguments[0].click();", label_el)
                                    time.sleep(random.uniform(3, 6)) # wait for UI and DOM to refresh
                                except Exception as e:
                                    logger.warning(f"Could not click L4 filter '{l4_filter_value}': {e}")
                            
                            # 2. Scroll
                            self.ps.scroll_all(driver, JIOMART_CFG["product_card"])
                            
                            # 3. Parse
                            records = self.psr.parse(driver, city, zone, pincode, cat_name, subcat_name)
                            
                            if records:
                                all_records.extend(records)
                                df_inc = pd.DataFrame(records, columns=PRODUCT_SCHEMA)
                                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                                out = RAW_DIR / f"jiomart_{city}_{pincode}_{ts}.csv"
                                df_inc.to_csv(out, index=False, encoding="utf-8-sig")
                            
                            time.sleep(random.uniform(4, 7))
                        except Exception as e:
                            logger.error(f"Error scraping {city}|{pincode}|{subcat_name}: {e}")

        if driver:
            self.bm.quit(driver)

        df = pd.DataFrame(all_records, columns=PRODUCT_SCHEMA)
        logger.info(f"Pipeline finished in {time.monotonic() - t0:.2f}s. Scraped {len(df)} total items.")
        return df

