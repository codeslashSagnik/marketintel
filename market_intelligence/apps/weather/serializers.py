"""
apps/weather/serializers.py
"""
from rest_framework import serializers
from .models import WeatherData


class WeatherDataSerializer(serializers.ModelSerializer):
    class Meta:
        model  = WeatherData
        fields = ["id", "city", "temperature", "humidity", "rainfall", "wind_speed", "recorded_at"]
        read_only_fields = ["id"]
