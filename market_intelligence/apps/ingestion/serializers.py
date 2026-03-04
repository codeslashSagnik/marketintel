"""
apps/ingestion/serializers.py
"""
from rest_framework import serializers
from .models import CompetitorPrice


class CompetitorPriceSerializer(serializers.ModelSerializer):
    product_sku  = serializers.CharField(source="product.sku_code", read_only=True)
    product_name = serializers.CharField(source="product.name",     read_only=True)

    class Meta:
        model  = CompetitorPrice
        fields = [
            "id", "product", "product_sku", "product_name",
            "platform", "price", "original_price", "discount_percent",
            "availability", "scraped_at",
        ]
        read_only_fields = ["id"]
