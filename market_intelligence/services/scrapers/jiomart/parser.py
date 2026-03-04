"""JioMart-specific Product Parser — DOM-based extraction using gtmEvents and CSS selectors."""
import re, logging
from typing import List, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

from market_intelligence.services.scrapers.jiomart.config import JIOMART_CFG

logger = logging.getLogger("scrapers.jiomart.parser")


class JioMartProductParser:
    """Uses exact DOM element matching for JioMart gtmEvents + fallback parsing."""

    def __init__(self, max_products: int = 200):
        self.cfg = JIOMART_CFG
        self.source = JIOMART_CFG["source"]
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
                continue

        # Dedup within batch
        seen, deduped = set(), []
        for r in records:
            key = (r['product_name'], r['pincode'])
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        return deduped
