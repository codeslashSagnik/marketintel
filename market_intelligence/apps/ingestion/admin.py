"""
apps/ingestion/admin.py
"""
import logging
from django.contrib import admin
from .models import CompetitorPrice

logger = logging.getLogger(__name__)


@admin.register(CompetitorPrice)
class CompetitorPriceAdmin(admin.ModelAdmin):
    list_display  = ("product", "platform", "price", "original_price", "discount_percent", "availability", "scraped_at")
    list_filter   = ("platform", "availability")
    search_fields = ("product__sku_code", "product__name", "platform")
    ordering      = ("-scraped_at",)
    list_select_related = ("product",)
    list_per_page = 100
