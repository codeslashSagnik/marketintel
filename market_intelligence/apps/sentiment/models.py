"""
apps/sentiment/models.py

Social media / news sentiment data fetched from Reddit, NewsAPI, etc.
Sentiment scores feed into the demand forecasting feature pipeline.
"""
import logging
from django.db import models

logger = logging.getLogger(__name__)


class SentimentData(models.Model):
    """
    Raw sentiment record for a keyword/topic from a social/news source.
    sentiment_score is in [-1.0, +1.0] (negative → positive).
    """
    SOURCE_CHOICES = [
        ("reddit",   "Reddit"),
        ("twitter",  "Twitter / X"),
        ("newsapi",  "NewsAPI"),
        ("google",   "Google Trends"),
        ("other",    "Other"),
    ]

    source          = models.CharField(max_length=64, choices=SOURCE_CHOICES, db_index=True)
    text            = models.TextField()
    sentiment_score = models.FloatField(
        help_text="Compound sentiment score in [-1.0, +1.0]"
    )
    keyword         = models.CharField(max_length=128, db_index=True)
    created_at      = models.DateTimeField(db_index=True, help_text="Original post/article date")
    fetched_at      = models.DateTimeField(auto_now_add=True,  db_index=True)

    class Meta:
        db_table = "sentiment_data"
        ordering = ["-fetched_at"]
        indexes = [
            models.Index(fields=["keyword", "fetched_at"], name="idx_sentiment_keyword_time"),
            models.Index(fields=["source",  "fetched_at"], name="idx_sentiment_source_time"),
        ]

    def __str__(self):
        return f"[{self.source}] {self.keyword} | score={self.sentiment_score:.3f} | {self.fetched_at:%Y-%m-%d %H:%M}"
