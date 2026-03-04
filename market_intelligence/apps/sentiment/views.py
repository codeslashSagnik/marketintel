"""
apps/sentiment/views.py
"""
import logging
from rest_framework import generics
from .models import SentimentData
from .serializers import SentimentDataSerializer

logger = logging.getLogger(__name__)


class SentimentDataListView(generics.ListAPIView):
    serializer_class = SentimentDataSerializer

    def get_queryset(self):
        qs = SentimentData.objects.order_by("-fetched_at")
        keyword = self.request.query_params.get("keyword")
        source  = self.request.query_params.get("source")
        if keyword:
            qs = qs.filter(keyword__icontains=keyword)
        if source:
            qs = qs.filter(source=source)
        return qs


class SentimentDataDetailView(generics.RetrieveAPIView):
    queryset         = SentimentData.objects.all()
    serializer_class = SentimentDataSerializer
