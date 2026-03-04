"""
apps/forecasting/serializers.py
"""
from rest_framework import serializers
from .models import ForecastResult


class ForecastResultSerializer(serializers.ModelSerializer):
    product_sku = serializers.CharField(source="product.sku_code", read_only=True)

    class Meta:
        model  = ForecastResult
        fields = [
            "id", "product", "product_sku", "city", "model_name",
            "forecast_date", "predicted_demand", "lower_bound", "upper_bound",
            "confidence", "generated_at",
        ]
        read_only_fields = ["id", "generated_at"]
