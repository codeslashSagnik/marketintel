"""
apps/forecasting/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class ForecastingConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.forecasting"
    verbose_name = "Forecasting"

    def ready(self):
        logger.info("ForecastingConfig ready")
