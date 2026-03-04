"""
apps/etl/views.py
"""
import logging
from rest_framework import generics
from .models import ETLRun
from .serializers import ETLRunSerializer

logger = logging.getLogger(__name__)


class ETLRunListView(generics.ListAPIView):
    serializer_class = ETLRunSerializer

    def get_queryset(self):
        qs = ETLRun.objects.order_by("-started_at")
        status = self.request.query_params.get("status")
        if status:
            qs = qs.filter(status=status)
        return qs


class ETLRunDetailView(generics.RetrieveAPIView):
    queryset         = ETLRun.objects.all()
    serializer_class = ETLRunSerializer
