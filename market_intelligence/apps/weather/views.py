"""
apps/weather/views.py
"""
import logging
from rest_framework import generics
from .models import WeatherData
from .serializers import WeatherDataSerializer

logger = logging.getLogger(__name__)


class WeatherDataListView(generics.ListAPIView):
    serializer_class = WeatherDataSerializer

    def get_queryset(self):
        qs = WeatherData.objects.order_by("-recorded_at")
        city = self.request.query_params.get("city")
        if city:
            qs = qs.filter(city__iexact=city)
        return qs


class WeatherDataDetailView(generics.RetrieveAPIView):
    queryset         = WeatherData.objects.all()
    serializer_class = WeatherDataSerializer
