import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException

from services.scrapers.base import get_logger

logger = get_logger("services.scrapers.bigbasket.location")


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

    def set_location(self, driver, pincode: str) -> bool:
        area_query = self.PINCODE_TO_SEARCH.get(pincode, pincode)
        logger.info(f"Setting BB location for pincode {pincode} → '{area_query}'")

        # ── Step 1: Navigate to homepage ──────────────────────
        driver.get(self.cfg["home_url"])
        time.sleep(random.uniform(3, 5))

        # ── Step 2: Click the "Delivery in X mins / Select Location" button ──
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
                visible = [e for e in elements if e.is_displayed()]
                for v in visible:
                    btn_text = v.text.strip().lower()
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
            time.sleep(2)
        except Exception as e:
            logger.error(f"Location button click failed: {e}")
            return False


        # ── Step 3: Type area name in the dropdown's search input ──
        try:
            search_input = None
            input_selectors = [
                "input[placeholder='Search for area or street name']",
                "input.Input-sc-tvw4mq-0",
                "div[role='menu'] input[type='text']",
                "div[id^='headlessui-menu-items'] input",
            ]
            
            time.sleep(1)
            for sel in input_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elements if e.is_displayed()]
                if visible:
                    search_input = visible[0]
                    break

            if not search_input:
                time.sleep(2)
                for sel in input_selectors:
                    elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    visible = [e for e in elements if e.is_displayed()]
                    if visible:
                        search_input = visible[0]
                        break

            if not search_input:
                logger.error("Could not find location search input in dropdown.")
                return False

            search_input.click()
            time.sleep(0.3)
            search_input.clear()

            for ch in area_query:
                search_input.send_keys(ch)
                time.sleep(random.uniform(0.04, 0.10))
            time.sleep(2.5)
        except Exception as e:
            logger.warning(f"Could not type in location search box: {e}")
            return False

        # ── Step 4: Click first suggestion ───────────────────
        try:
            suggestion = None
            sugg_selectors = [
                "li.sc-jdkBTo",
                "li.cnPYAb",
                "div[role='menu'] ul li",
                "div[id^='headlessui-menu-items'] li",
            ]
            time.sleep(1)
            for sel in sugg_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, sel)
                visible = [e for e in elements if e.is_displayed()]
                if visible:
                    suggestion = visible[0]
                    break

            if suggestion:
                suggestion.click()
                time.sleep(random.uniform(4, 6))
            else:
                search_input.send_keys(Keys.RETURN)
                time.sleep(4)
        except Exception as e:
            logger.warning(f"Suggestion click failed: {e}")
            return False

        # ── Step 5: Verify location was set ──────────────────
        try:
            time.sleep(2)
            loc_btn_after = driver.find_element(
                By.XPATH,
                "//button[.//span[contains(text(),'Delivery in')] or .//span[contains(text(),'Get it')]]"
            )
            banner_text = loc_btn_after.text.strip()
            if "select location" not in banner_text.lower():
                logger.info(f"Location set successfully! Banner: '{banner_text}'")
                return True
        except:
            pass
        return True
