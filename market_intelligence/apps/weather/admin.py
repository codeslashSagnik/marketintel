"""
apps/weather/admin.py
"""
import logging
from django.contrib import admin
from .models import WeatherData

logger = logging.getLogger(__name__)


@admin.register(WeatherData)
class WeatherDataAdmin(admin.ModelAdmin):
    list_display  = ("city", "temperature", "humidity", "rainfall", "wind_speed", "recorded_at")
    list_filter   = ("city",)
    search_fields = ("city",)
    ordering      = ("-recorded_at",)
    list_per_page = 200
