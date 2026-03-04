"""
apps/sales/serializers.py
"""
from rest_framework import serializers
from .models import HistoricalSales


class HistoricalSalesSerializer(serializers.ModelSerializer):
    product_sku  = serializers.CharField(source="product.sku_code", read_only=True)
    product_name = serializers.CharField(source="product.name",     read_only=True)

    class Meta:
        model  = HistoricalSales
        fields = ["id", "date", "product", "product_sku", "product_name", "city", "sales"]
        read_only_fields = ["id"]
