"""
apps/sentiment/admin.py
"""
import logging
from django.contrib import admin
from .models import SentimentData

logger = logging.getLogger(__name__)


@admin.register(SentimentData)
class SentimentDataAdmin(admin.ModelAdmin):
    list_display  = ("source", "keyword", "sentiment_score", "created_at", "fetched_at")
    list_filter   = ("source", "keyword")
    search_fields = ("keyword", "text")
    ordering      = ("-fetched_at",)
    list_per_page = 100
