"""
apps/ingestion/models.py

Competitor pricing data scraped from external platforms (BigBasket, Blinkit, etc.)
"""
import logging
from django.db import models
from apps.products.models import Product

logger = logging.getLogger(__name__)


class CompetitorPrice(models.Model):
    """
    Tracks the price of a product on a competitor's platform at a given point in time.
    scraped_at is indexed for fast time-range queries.
    """
    PLATFORM_CHOICES = [
        ("bigbasket",  "BigBasket"),
        ("blinkit",    "Blinkit"),
        ("zepto",      "Zepto"),
        ("swiggy",     "Swiggy Instamart"),
        ("amazon",     "Amazon"),
        ("flipkart",   "Flipkart"),
        ("other",      "Other"),
    ]

    product          = models.ForeignKey(
        Product, on_delete=models.CASCADE,
        related_name="competitor_prices", db_index=True
    )
    platform         = models.CharField(max_length=64, choices=PLATFORM_CHOICES, db_index=True)
    price            = models.DecimalField(max_digits=10, decimal_places=2)
    original_price   = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    discount_percent = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    availability     = models.BooleanField(default=True)
    scraped_at       = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "ingestion_competitor_price"
        ordering = ["-scraped_at"]
        indexes = [
            models.Index(fields=["platform", "scraped_at"], name="idx_cp_platform_time"),
            models.Index(fields=["product", "platform"],   name="idx_cp_product_platform"),
        ]

    def __str__(self):
        return f"{self.product.sku_code} | {self.platform} | ₹{self.price} @ {self.scraped_at:%Y-%m-%d %H:%M}"
