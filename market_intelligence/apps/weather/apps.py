"""
apps/weather/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class WeatherConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.weather"
    verbose_name = "Weather"

    def ready(self):
        logger.info("WeatherConfig ready")
