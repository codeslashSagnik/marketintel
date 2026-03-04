"""
apps/sentiment/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class SentimentConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.sentiment"
    verbose_name = "Sentiment"

    def ready(self):
        logger.info("SentimentConfig ready")
