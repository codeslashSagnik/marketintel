"""
config/urls.py

Root URL configuration for Market Intelligence Platform.
Each app mounts its own router under a versioned API prefix.
"""

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # Django admin (keep for monitoring / data exploration)
    path("admin/", admin.site.urls),

    # ── API v1 ────────────────────────────────────────────────────────────────
    path("api/v1/products/",    include("apps.products.urls")),
    path("api/v1/ingestion/",   include("apps.ingestion.urls")),
    path("api/v1/weather/",     include("apps.weather.urls")),
    path("api/v1/sentiment/",   include("apps.sentiment.urls")),
    path("api/v1/sales/",       include("apps.sales.urls")),
    path("api/v1/etl/",         include("apps.etl.urls")),
    path("api/v1/forecasting/", include("apps.forecasting.urls")),
    path("api/v1/monitoring/",  include("apps.monitoring.urls")),
]
