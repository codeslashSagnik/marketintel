"""
apps/monitoring/serializers.py
"""
from rest_framework import serializers
from .models import IngestionLog


class IngestionLogSerializer(serializers.ModelSerializer):
    class Meta:
        model  = IngestionLog
        fields = [
            "id", "source", "records_processed",
            "status", "latency_seconds", "error_message", "created_at",
        ]
        read_only_fields = ["id", "created_at"]
