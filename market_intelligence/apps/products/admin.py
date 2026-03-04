"""
apps/products/admin.py
"""
import logging
from django.contrib import admin
from .models import Product

logger = logging.getLogger(__name__)


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display  = ("sku_code", "name", "brand", "category", "is_active", "created_at")
    list_filter   = ("category", "brand", "is_active")
    search_fields = ("sku_code", "name", "brand")
    ordering      = ("category", "name")
    list_per_page = 50
