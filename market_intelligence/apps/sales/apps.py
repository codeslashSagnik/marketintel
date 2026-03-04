"""
apps/sales/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class SalesConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.sales"
    verbose_name = "Sales"

    def ready(self):
        logger.info("SalesConfig ready")
