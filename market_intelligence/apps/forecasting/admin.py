"""
apps/forecasting/admin.py
"""
import logging
from django.contrib import admin
from .models import ForecastResult

logger = logging.getLogger(__name__)


@admin.register(ForecastResult)
class ForecastResultAdmin(admin.ModelAdmin):
    list_display  = ("product", "city", "model_name", "forecast_date", "predicted_demand", "confidence", "generated_at")
    list_filter   = ("model_name", "city")
    search_fields = ("product__sku_code", "product__name", "city")
    ordering      = ("-forecast_date",)
    list_select_related = ("product",)
    list_per_page = 100
