"""
apps/monitoring/admin.py
"""
import logging
from django.contrib import admin
from .models import IngestionLog

logger = logging.getLogger(__name__)


@admin.register(IngestionLog)
class IngestionLogAdmin(admin.ModelAdmin):
    list_display  = ("source", "records_processed", "status", "latency_seconds", "created_at")
    list_filter   = ("status", "source")
    search_fields = ("source",)
    ordering      = ("-created_at",)
    list_per_page = 200
    readonly_fields = ("created_at",)
