"""
BigBasket Location Button Diagnostic
=====================================
Takes a screenshot + dumps header HTML to see what Selenium actually sees.
"""
import os, time, random
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

DEBUG_DIR = Path("E:/cv projects/real_time-market-intelligence/data/debug")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

CHROME_VERSION = "145.0.7632.76"

def resolve_driver():
    p = ChromeDriverManager(driver_version=CHROME_VERSION).install()
    if not p.endswith(".exe"):
        base = os.path.dirname(p) if os.path.isfile(p) else p
        for root, _, files in os.walk(base):
            if "chromedriver.exe" in files:
                return os.path.join(root, "chromedriver.exe")
    return p

def run_diagnostic():
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    opts.add_argument(f"--user-agent={ua}")
    opts.add_argument("--window-size=1920,1080")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=Service(resolve_driver()), options=opts)
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": ua})
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    """})
    print("[OK] Browser created")

    # ── Navigate to BigBasket ────────────────────────────
    print("[...] Loading bigbasket.com")
    driver.get("https://www.bigbasket.com/")
    time.sleep(6)  # Wait generously

    # ── Screenshot 1: Immediately after load ─────────────
    ss1 = str(DEBUG_DIR / "diag_after_load.png")
    driver.save_screenshot(ss1)
    print(f"[OK] Screenshot saved: {ss1}")

    # ── Dump page title & URL ────────────────────────────
    print(f"[INFO] Page title: {driver.title}")
    print(f"[INFO] Current URL: {driver.current_url}")

    # ── Check for popups / overlays ──────────────────────
    print("\n=== POPUP / OVERLAY CHECK ===")
    overlay_selectors = [
        "div[class*='overlay']",
        "div[class*='modal']",
        "div[class*='popup']",
        "div[class*='consent']",
        "div[class*='dialog']",
        "div[role='dialog']",
        "div[data-testid*='modal']",
    ]
    for sel in overlay_selectors:
        els = driver.find_elements(By.CSS_SELECTOR, sel)
        visible = [e for e in els if e.is_displayed()]
        if visible:
            print(f"  ⚠️  FOUND visible overlay: '{sel}' ({len(visible)} elements)")
            for v in visible[:2]:
                print(f"       Text: '{v.text[:100]}'")
                print(f"       Class: {v.get_attribute('class')[:80]}")
        else:
            if els:
                print(f"  ✓ '{sel}' exists but hidden ({len(els)} elements)")

    # ── Find ALL buttons on the page ─────────────────────
    print("\n=== ALL BUTTONS ON PAGE ===")
    all_btns = driver.find_elements(By.TAG_NAME, "button")
    print(f"  Total buttons found: {len(all_btns)}")
    for i, btn in enumerate(all_btns[:20]):
        text = btn.text.strip().replace("\n", " | ")[:60]
        cls = btn.get_attribute("class")[:60]
        btn_id = btn.get_attribute("id") or ""
        visible = btn.is_displayed()
        enabled = btn.is_enabled()
        print(f"  [{i}] id='{btn_id}' class='{cls}' visible={visible} enabled={enabled}")
        print(f"       text='{text}'")

    # ── Try to find the location button specifically ─────
    print("\n=== LOCATION BUTTON SEARCH ===")
    
    # Method A: find_elements (no wait, just check DOM)
    test_selectors = {
        "CSS: button[id^='headlessui-menu-button-']": (By.CSS_SELECTOR, "button[id^='headlessui-menu-button-']"),
        "CSS: button.sc-gweoQa": (By.CSS_SELECTOR, "button.sc-gweoQa"),
        "CSS: button.ecHHQH": (By.CSS_SELECTOR, "button.ecHHQH"),
        "XPath: Select Location text": (By.XPATH, "//button[.//span[contains(text(),'Select Location')]]"),
        "XPath: Delivery in text": (By.XPATH, "//button[.//span[contains(text(),'Delivery in')]]"),
        "XPath: any button with span": (By.XPATH, "//button[.//span]"),
    }
    
    for name, (by, sel) in test_selectors.items():
        els = driver.find_elements(by, sel)
        print(f"\n  [{name}]")
        print(f"    Found: {len(els)} element(s)")
        for j, el in enumerate(els[:5]):
            print(f"    [{j}] visible={el.is_displayed()} enabled={el.is_enabled()}")
            print(f"         text='{el.text.strip()[:60]}'")
            print(f"         class='{el.get_attribute('class')[:60]}'")

    # Method B: Check with element_to_be_clickable vs presence_of_element_located
    print("\n=== CLICKABLE vs PRESENT CHECK ===")
    for sel_name, sel in [
        ("headlessui button", "button[id^='headlessui-menu-button-']"),
        ("Select Location XPath", "//button[.//span[contains(text(),'Select Location')]]"),
    ]:
        by = By.XPATH if sel.startswith("//") else By.CSS_SELECTOR
        # presence check
        try:
            el = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((by, sel))
            )
            print(f"  ✅ PRESENT: '{sel_name}' → text='{el.text.strip()[:50]}'")
        except:
            print(f"  ❌ NOT PRESENT: '{sel_name}'")
        
        # clickable check
        try:
            el = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((by, sel))
            )
            print(f"  ✅ CLICKABLE: '{sel_name}' → text='{el.text.strip()[:50]}'")
        except:
            print(f"  ❌ NOT CLICKABLE: '{sel_name}'")

    # ── Dump header HTML ──────────────────────────────────
    print("\n=== HEADER HTML DUMP ===")
    try:
        header = driver.find_element(By.TAG_NAME, "header")
        header_html = header.get_attribute("outerHTML")
        dump_path = DEBUG_DIR / "diag_header_dump.html"
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(header_html)
        print(f"  Header HTML saved to: {dump_path}")
        print(f"  Header HTML length: {len(header_html)} chars")
    except:
        print("  ⚠️ No <header> element found!")
        # Try dumping first 5000 chars of body
        try:
            body = driver.find_element(By.TAG_NAME, "body")
            body_html = body.get_attribute("outerHTML")[:5000]
            dump_path = DEBUG_DIR / "diag_body_top.html"
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write(body_html)
            print(f"  Body top HTML saved to: {dump_path}")
        except:
            print("  ⚠️ Could not dump body either!")

    # ── Full page source dump ─────────────────────────────
    full_dump = DEBUG_DIR / "diag_full_page.html"
    with open(full_dump, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print(f"\n[OK] Full page source saved to: {full_dump}")

    input("\nPress Enter to close browser...")
    driver.quit()

if __name__ == "__main__":
    run_diagnostic()
