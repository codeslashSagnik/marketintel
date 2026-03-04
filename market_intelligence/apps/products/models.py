"""
apps/products/models.py

Product master data — the single source of truth for all SKUs.
"""
import logging
from django.db import models

logger = logging.getLogger(__name__)


class Product(models.Model):
    """
    Master product catalog entry.
    Each row represents a unique sellable SKU.
    """
    name      = models.CharField(max_length=255, db_index=True)
    brand     = models.CharField(max_length=128, db_index=True)
    category  = models.CharField(max_length=128, db_index=True)
    sku_code  = models.CharField(max_length=64, unique=True)
    is_active = models.BooleanField(default=True, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "products_product"
        ordering = ["category", "name"]
        indexes = [
            models.Index(fields=["category", "brand"], name="idx_product_cat_brand"),
        ]

    def __str__(self):
        return f"[{self.sku_code}] {self.name} — {self.brand}"
