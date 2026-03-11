# Scraper Performance and Optimizations

This document outlines the brainstorming and implemented changes for stabilizing and speeding up the Market Intelligence Scraper pipeline, particularly for JioMart.

## 1. JioMart L4 Filter Redundancy Removal

**The Problem:**
During the application of L4 filters on JioMart (like "Chyawanprash"), the `_apply_l4_filter` method would first look for the filter natively in the sidebar. If not found, a naive `else:` block immediately searched for a "Show More" (`+X More`) modal button and forced open a pop-up. If the filter or the modal button didn't actually exist in the DOM, Selenium would waste ~10 seconds waiting via `WebDriverWait` before finally throwing a `TimeoutException` or `NoSuchElementException`, then failing over.

**The Solution:**
We removed the redundant modal pop-up logic completely from the `JioMartScraper._apply_l4_filter` method. Now, the scraper relies exclusively on checking if the target L4 subcategory filter is immediately available and `is_displayed()` in the sidebar. 
- **Result**: Drastically reduces scraper wait times (saving up to 10 seconds per category) by failing fast and avoiding fruitless implicit waits when the specific category page doesn't require a modal.

## 2. Fixing NULL Prices in PostgreSQL

**The Problem:**
BigBasket was inserting proper `(name, brand, price, mrp)` tuples, but JioMart was persistently streaming `null` values into downstream Spark and Postgres tables for prices and MRPs.

**The Solution:**
Upon investigation of the `JioMartProductParser`, the CSS selection for prices (`.plp-card-details-price`) was returning a DOM element, but extracting the `.text` property of the `BeautifulSoup` node yielded formatting artifacts (like `₹` or inner spans) that broke the float casting logic. 
- **Fix**: Replaced `.text` with `.get_text(separator=" ").strip()` inside `parser.py` prior to passing it to `_clean_price()`. This guarantees a clean string representation, eliminating `None` returns and ensuring Postgres receives decimal values.

## 3. General Speed Optimizations

To further increase scraper throughput without triggering advanced WAF barriers, we employ the following techniques:
1. **Targeted Clicks (`is_displayed`)**: Only initiate explicit Selenium `.click()` interactions conditionally. We integrated checks for `is_expanded()` prior to interacting with parent category groups on JioMart.
2. **Headless Execution**: Maintain the `-headless=new` flag to ensure the GPU doesn't throttle background CPU processing.
3. **Pincode Rotation & Refresh (`MAX_SUBCATEGORIES`)**: Rather than deeply crawling 50 pages of a single L4 category, we distribute the workload evenly by enforcing a shallow hard-limit `self.max_products` per subcategory, ensuring high data velocity across numerous geolocation `pincodes`.
4. **Celery Worker Concurrency**: By leveraging `celery -A celery_app worker -P solo` across multiple distributed terminals or Docker containers, we achieve high spatial parallelism.
