"""
apps/forecasting/urls.py
"""
from django.urls import path
from . import views

app_name = "forecasting"

urlpatterns = [
    path("",          views.ForecastResultListView.as_view(),   name="forecast-list"),
    path("<int:pk>/", views.ForecastResultDetailView.as_view(), name="forecast-detail"),
]
