"""
apps/products/apps.py
"""
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class ProductsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.products"
    verbose_name = "Products"

    def ready(self):
        logger.info("ProductsConfig ready")
