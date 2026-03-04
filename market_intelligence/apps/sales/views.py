"""
apps/sales/views.py
"""
import logging
from rest_framework import generics
from .models import HistoricalSales
from .serializers import HistoricalSalesSerializer

logger = logging.getLogger(__name__)


class HistoricalSalesListView(generics.ListAPIView):
    serializer_class = HistoricalSalesSerializer

    def get_queryset(self):
        qs = HistoricalSales.objects.select_related("product").order_by("-date")
        city = self.request.query_params.get("city")
        if city:
            qs = qs.filter(city__iexact=city)
        return qs


class HistoricalSalesDetailView(generics.RetrieveAPIView):
    queryset         = HistoricalSales.objects.select_related("product")
    serializer_class = HistoricalSalesSerializer
