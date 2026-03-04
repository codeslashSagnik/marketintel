"""
apps/monitoring/urls.py
"""
from django.urls import path
from . import views

app_name = "monitoring"

urlpatterns = [
    path("logs/",         views.IngestionLogListView.as_view(),   name="log-list"),
    path("logs/<int:pk>/",views.IngestionLogDetailView.as_view(), name="log-detail"),
]
