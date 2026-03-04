"""
Shared Browser Manager — Headless Chrome with anti-detection measures.
Used by all Selenium-based scrapers (JioMart, BigBasket).
"""
import os, time, random, logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger("scrapers.browser")

CHROME_VERSION = "145.0.7632.76"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


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


class PageScroller:
    """Scrolls infinite-scroll pages to lazy-load all product cards."""
    def __init__(self, max_scrolls: int = 60):
        self.max_scrolls = max_scrolls

    def scroll_all(self, driver: webdriver.Chrome, card_selector: str) -> int:
        from selenium.webdriver.common.by import By
        last_count = 0
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
