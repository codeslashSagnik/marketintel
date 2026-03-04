"""
apps/weather/urls.py
"""
from django.urls import path
from . import views

app_name = "weather"

urlpatterns = [
    path("",          views.WeatherDataListView.as_view(),   name="weather-list"),
    path("<int:pk>/", views.WeatherDataDetailView.as_view(), name="weather-detail"),
]
