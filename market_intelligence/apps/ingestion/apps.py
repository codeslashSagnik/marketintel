"""
apps/ingestion/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class IngestionConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.ingestion"
    verbose_name = "Ingestion"

    def ready(self):
        logger.info("IngestionConfig ready")
