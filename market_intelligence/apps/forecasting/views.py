"""
apps/forecasting/views.py
"""
import logging
from rest_framework import generics
from .models import ForecastResult
from .serializers import ForecastResultSerializer

logger = logging.getLogger(__name__)


class ForecastResultListView(generics.ListAPIView):
    serializer_class = ForecastResultSerializer

    def get_queryset(self):
        qs = ForecastResult.objects.select_related("product").order_by("-forecast_date")
        city       = self.request.query_params.get("city")
        model_name = self.request.query_params.get("model")
        if city:
            qs = qs.filter(city__iexact=city)
        if model_name:
            qs = qs.filter(model_name=model_name)
        return qs


class ForecastResultDetailView(generics.RetrieveAPIView):
    queryset         = ForecastResult.objects.select_related("product")
    serializer_class = ForecastResultSerializer
