"""
apps/sales/urls.py
"""
from django.urls import path
from . import views

app_name = "sales"

urlpatterns = [
    path("",          views.HistoricalSalesListView.as_view(),   name="sales-list"),
    path("<int:pk>/", views.HistoricalSalesDetailView.as_view(), name="sales-detail"),
]
