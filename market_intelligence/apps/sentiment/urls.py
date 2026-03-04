"""
apps/sentiment/urls.py
"""
from django.urls import path
from . import views

app_name = "sentiment"

urlpatterns = [
    path("",          views.SentimentDataListView.as_view(),   name="sentiment-list"),
    path("<int:pk>/", views.SentimentDataDetailView.as_view(), name="sentiment-detail"),
]
