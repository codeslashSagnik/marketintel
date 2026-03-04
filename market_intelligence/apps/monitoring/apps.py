"""
apps/monitoring/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class MonitoringConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.monitoring"
    verbose_name = "Monitoring"

    def ready(self):
        logger.info("MonitoringConfig ready")
