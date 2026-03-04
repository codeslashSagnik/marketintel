"""
apps/etl/serializers.py
"""
from rest_framework import serializers
from .models import ETLRun


class ETLRunSerializer(serializers.ModelSerializer):
    class Meta:
        model  = ETLRun
        fields = [
            "id", "pipeline_name", "status", "rows_input", "rows_output",
            "error_message", "started_at", "completed_at", "duration_seconds",
        ]
        read_only_fields = ["id", "started_at", "completed_at", "duration_seconds"]
