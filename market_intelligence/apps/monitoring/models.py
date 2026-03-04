"""
apps/monitoring/models.py

Operational observability — records metrics for every ingestion pipeline run.
Used for dashboards, alerting, and SLA tracking.
"""
import logging
from django.db import models

logger = logging.getLogger(__name__)


class IngestionLog(models.Model):
    """
    One row per pipeline execution (scraper run, API fetch, CSV import).
    Tracks records processed, latency, and any errors.
    """
    STATUS_CHOICES = [
        ("success", "Success"),
        ("partial", "Partial"),
        ("failed",  "Failed"),
    ]

    source             = models.CharField(max_length=128, db_index=True,
                                          help_text="Pipeline / source name, e.g. 'bigbasket_scraper'")
    records_processed  = models.PositiveIntegerField(default=0)
    status             = models.CharField(max_length=16, choices=STATUS_CHOICES, db_index=True)
    latency_seconds    = models.FloatField(null=True, blank=True)
    error_message      = models.TextField(blank=True, default="")
    created_at         = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "monitoring_ingestion_log"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["source", "created_at"], name="idx_inglog_source_time"),
            models.Index(fields=["status", "created_at"], name="idx_inglog_status_time"),
        ]

    def __str__(self):
        return (
            f"source={self.source} records={self.records_processed} "
            f"status={self.status} latency={self.latency_seconds}s @ {self.created_at:%Y-%m-%d %H:%M}"
        )
