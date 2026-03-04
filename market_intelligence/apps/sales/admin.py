"""
apps/sales/admin.py
"""
import logging
from django.contrib import admin
from .models import HistoricalSales

logger = logging.getLogger(__name__)


@admin.register(HistoricalSales)
class HistoricalSalesAdmin(admin.ModelAdmin):
    list_display  = ("date", "product", "city", "sales")
    list_filter   = ("city",)
    search_fields = ("product__sku_code", "product__name", "city")
    ordering      = ("-date",)
    list_select_related = ("product",)
    list_per_page = 200
