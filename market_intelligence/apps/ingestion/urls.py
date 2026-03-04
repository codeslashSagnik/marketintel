"""
apps/ingestion/urls.py
"""
from django.urls import path
from . import views

app_name = "ingestion"

urlpatterns = [
    path("prices/",         views.CompetitorPriceListView.as_view(),   name="price-list"),
    path("prices/<int:pk>/",views.CompetitorPriceDetailView.as_view(), name="price-detail"),
]
