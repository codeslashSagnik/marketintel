"""
apps/sentiment/serializers.py
"""
from rest_framework import serializers
from .models import SentimentData


class SentimentDataSerializer(serializers.ModelSerializer):
    class Meta:
        model  = SentimentData
        fields = ["id", "source", "text", "sentiment_score", "keyword", "created_at", "fetched_at"]
        read_only_fields = ["id", "fetched_at"]
