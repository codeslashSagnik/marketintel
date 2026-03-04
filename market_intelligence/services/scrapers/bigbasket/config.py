"""BigBasket-specific configuration — selectors, URLs, and static fallbacks."""

BIGBASKET_CFG = {
    "source": "bigbasket",
    "home_url": "https://www.bigbasket.com/",

    # ── Location selectors ────────────────────────────────
    "location_btn": "button[id^='headlessui-menu-button']",
    "search_input": "input[placeholder*='Search for area']",
    "suggestion_item": "li[id^='headlessui-menu-item']",

    # ── Product card selectors ────────────────────────────
    # BigBasket uses React styled-components.
    "product_card": "div[class*='SKUDeck___StyledDiv']",
    "product_card_fallback": "li.SKUDeck, div.SKUDeck, div[data-qa='product']",
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

# ── Dynamic Catalog Constants ──
TARGET_L1_CATEGORIES = [
    "Fruits & Vegetables",
    "Bakery, Cakes & Dairy",
    "Snacks & Branded Foods"
]
