"""
apps/etl/urls.py
"""
from django.urls import path
from . import views

app_name = "etl"

urlpatterns = [
    path("runs/",         views.ETLRunListView.as_view(),   name="etl-run-list"),
    path("runs/<int:pk>/",views.ETLRunDetailView.as_view(), name="etl-run-detail"),
]
