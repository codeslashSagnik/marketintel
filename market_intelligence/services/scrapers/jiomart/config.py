"""JioMart-specific configuration — selectors, URLs, and site-specific settings."""

JIOMART_CFG = {
    "source": "jiomart",
    "home_url": "https://www.jiomart.com/",
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
    "l2_category_item": "#categories_filter .ais-HierarchicalMenu-item--child .ais-HierarchicalMenu-label",
    "subcategory_item": "#attributes\\.category_level_4_filter .ais-RefinementList-item",
    "subcategory_label": ".ais-RefinementList-labelText",
    "subcategory_checkbox": ".ais-refinement-list--checkbox",
    "show_more_subcats": ".filters-box .show_more button",
}

# L2 categories to scrape deep subcategories from
TARGET_L2_CATEGORIES = [
    "Biscuits, Drinks & Packaged Foods",
    "Dairy & Bakery",
]
