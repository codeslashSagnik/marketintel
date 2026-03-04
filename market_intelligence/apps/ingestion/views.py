"""
apps/ingestion/views.py
"""
import logging
from rest_framework import generics
from .models import CompetitorPrice
from .serializers import CompetitorPriceSerializer

logger = logging.getLogger(__name__)


class CompetitorPriceListView(generics.ListAPIView):
    serializer_class = CompetitorPriceSerializer

    def get_queryset(self):
        qs = CompetitorPrice.objects.select_related("product").order_by("-scraped_at")
        platform = self.request.query_params.get("platform")
        if platform:
            qs = qs.filter(platform=platform)
        return qs


class CompetitorPriceDetailView(generics.RetrieveAPIView):
    queryset         = CompetitorPrice.objects.select_related("product")
    serializer_class = CompetitorPriceSerializer
