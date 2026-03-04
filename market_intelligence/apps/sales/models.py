"""
apps/sales/models.py

Historical sales data — loaded from CSV exports or internal ERP.
Used as the primary training signal for demand forecasting.
"""
import logging
from django.db import models
from apps.products.models import Product

logger = logging.getLogger(__name__)


class HistoricalSales(models.Model):
    """
    Daily city-level sales quantity per product.
    Bulk-created during CSV ingestion — avoid single-insert loops.
    """
    date    = models.DateField(db_index=True)
    product = models.ForeignKey(
        Product, on_delete=models.CASCADE,
        related_name="historical_sales", db_index=True
    )
    city    = models.CharField(max_length=128, db_index=True)
    sales   = models.DecimalField(max_digits=12, decimal_places=2, help_text="Units sold")

    class Meta:
        db_table = "sales_historical"
        ordering = ["-date"]
        unique_together = [("date", "product", "city")]
        indexes = [
            models.Index(fields=["product", "date"],  name="idx_sales_product_date"),
            models.Index(fields=["city",    "date"],  name="idx_sales_city_date"),
        ]

    def __str__(self):
        return f"{self.date} | {self.product.sku_code} | {self.city} | qty={self.sales}"
