"""
apps/etl/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class EtlConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.etl"
    verbose_name = "ETL"

    def ready(self):
        logger.info("EtlConfig ready")
