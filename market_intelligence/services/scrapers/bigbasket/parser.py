import re
from datetime import datetime
from typing import List, Optional
from bs4 import BeautifulSoup

from services.scrapers.base import get_logger

logger = get_logger("services.scrapers.bigbasket.parser")


class BBProductParser:
    """Parses BigBasket product cards from the listing page DOM."""

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
        potential = soup.select("div:has(> h3):has(span:contains('₹'))")
        if potential:
            logger.info(f"Found {len(potential)} cards via heuristic")
            return potential

        logger.warning("No product cards found with any strategy!")
        return []

    def _extract_text(self, item, selectors: str) -> Optional[str]:
        for sel in selectors.split(","):
            sel = sel.strip()
            el = item.select_one(sel)
            if el and el.text.strip():
                return el.text.strip()
        return None

    def _extract_image(self, item) -> Optional[str]:
        for sel in ["img[data-qa='product_image']", "img[class*='ProductImage']", "img"]:
            img = item.select_one(sel)
            if img:
                src = img.get("data-src") or img.get("src") or img.get("srcset", "").split(",")[0].split(" ")[0]
                if src and src.startswith("http"):
                    return src
        return None

    def parse(self, driver, city: str, zone: str, pincode: str,
              cat_name: str, subcat_name: str) -> List[dict]:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = self._find_product_cards(soup)
        ts = datetime.utcnow().isoformat() + "Z"

        records = []
        for item in items[:self.max_products]:
            try:
                # ── Product Name & Variant ──────────────────────────
                name_el = item.select_one("h3.block, h3[class*='line-clamp'], h3")
                name = name_el.text.strip() if name_el else None
                variant = None

                if name and "-" in name:
                    parts = name.split("-", 1)
                    variant = parts[1].strip()

                # ── Brand ─────────────────────────────────
                brand_el = item.select_one("span[class*='BrandName'], span[class*='brand']")
                brand = brand_el.text.strip() if brand_el else None

                # ── Prices & Discount ────────────────────────────────
                mrp = None
                price = None
                discount = None

                price_elements = item.select("span:contains('₹'), div:has(span:contains('₹'))")
                for el in price_elements:
                    text = el.text.strip()
                    if not text or '₹' not in text: continue

                    val = self._clean_price(text.split('₹')[-1])
                    if not val: continue

                    if el.name in ['s', 'del'] or el.find_parent(['s', 'del']) or \
                       'line-through' in el.get('style', '') or 'line-through' in el.get('class', []):
                        mrp = val
                    else:
                        if price is not None:
                            if val < price:
                                mrp = price
                                price = val
                            elif val > price:
                                mrp = val
                        else:
                            price = val

                if price and not mrp: mrp = price
                if mrp and not price: price = mrp

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
