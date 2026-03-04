"""
apps/forecasting/models.py

Demand forecast results produced by ML models.
"""
import logging
from django.db import models
from apps.products.models import Product

logger = logging.getLogger(__name__)


class ForecastResult(models.Model):
    """
    Stores the output of a demand forecasting model run.
    One row = one product × city × forecast horizon.
    """
    MODEL_CHOICES = [
        ("xgboost",   "XGBoost"),
        ("prophet",   "Prophet"),
        ("lstm",      "LSTM"),
        ("arima",     "ARIMA"),
        ("ensemble",  "Ensemble"),
    ]

    product          = models.ForeignKey(
        Product, on_delete=models.CASCADE,
        related_name="forecasts", db_index=True
    )
    city             = models.CharField(max_length=128, db_index=True)
    model_name       = models.CharField(max_length=64, choices=MODEL_CHOICES, db_index=True)
    forecast_date    = models.DateField(db_index=True, help_text="Date the forecast is for")
    predicted_demand = models.DecimalField(max_digits=12, decimal_places=2)
    lower_bound      = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    upper_bound      = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    confidence       = models.FloatField(null=True, blank=True, help_text="Model confidence [0,1]")
    generated_at     = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "forecasting_result"
        ordering = ["-forecast_date"]
        unique_together = [("product", "city", "model_name", "forecast_date")]
        indexes = [
            models.Index(fields=["product", "forecast_date"], name="idx_forecast_product_date"),
            models.Index(fields=["city",    "forecast_date"], name="idx_forecast_city_date"),
        ]

    def __str__(self):
        return (
            f"{self.product.sku_code} | {self.city} | {self.model_name} | "
            f"date={self.forecast_date} | demand={self.predicted_demand}"
        )
