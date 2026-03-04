"""
apps/etl/models.py

ETL pipeline tracking — records each transformation run for auditing.
"""
import logging
from django.db import models

logger = logging.getLogger(__name__)


class ETLRun(models.Model):
    """
    Audit log for each ETL / feature-generation pipeline execution.
    """
    STATUS_CHOICES = [
        ("pending",   "Pending"),
        ("running",   "Running"),
        ("success",   "Success"),
        ("failed",    "Failed"),
    ]

    pipeline_name    = models.CharField(max_length=128, db_index=True)
    status           = models.CharField(max_length=16, choices=STATUS_CHOICES, default="pending", db_index=True)
    rows_input       = models.PositiveIntegerField(default=0)
    rows_output      = models.PositiveIntegerField(default=0)
    error_message    = models.TextField(blank=True, default="")
    started_at       = models.DateTimeField(db_index=True)
    completed_at     = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "etl_run"
        ordering = ["-started_at"]
        indexes = [
            models.Index(fields=["pipeline_name", "started_at"], name="idx_etl_pipeline_time"),
        ]

    def __str__(self):
        return f"{self.pipeline_name} | {self.status} | {self.started_at:%Y-%m-%d %H:%M}"
