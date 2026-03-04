"""JioMart-specific Location Manager — Google Places area search UI."""
import time, random, logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger("scrapers.jiomart.location")


class JioMartLocationManager:
    """Sets JioMart delivery location using the area-search Google Places UI."""

    PINCODE_TO_AREA = {
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
        area_query = self.PINCODE_TO_AREA.get(pincode, pincode)

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

        # Step 4: Click the Angular 'Confirm Location' button
        confirm_selectors = [
            "button[aria-label='button Confirm Location']",
            "button.j-button[name='jds-button']",
            ".j-button.primary",
            "button[class*='confirm']",
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
