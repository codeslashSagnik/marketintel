"""
apps/products/serializers.py
"""
from rest_framework import serializers
from .models import Product


class ProductSerializer(serializers.ModelSerializer):
    class Meta:
        model  = Product
        fields = ["id", "name", "brand", "category", "sku_code", "is_active", "created_at", "updated_at"]
        read_only_fields = ["id", "created_at", "updated_at"]
