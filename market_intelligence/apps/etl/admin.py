"""
apps/etl/admin.py
"""
import logging
from django.contrib import admin
from .models import ETLRun

logger = logging.getLogger(__name__)


@admin.register(ETLRun)
class ETLRunAdmin(admin.ModelAdmin):
    list_display  = ("pipeline_name", "status", "rows_input", "rows_output", "duration_seconds", "started_at")
    list_filter   = ("status", "pipeline_name")
    search_fields = ("pipeline_name",)
    ordering      = ("-started_at",)
    list_per_page = 100
    readonly_fields = ("started_at", "completed_at", "duration_seconds")
