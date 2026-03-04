"""
apps/monitoring/views.py
"""
import logging
from rest_framework import generics
from .models import IngestionLog
from .serializers import IngestionLogSerializer

logger = logging.getLogger(__name__)


class IngestionLogListView(generics.ListAPIView):
    serializer_class = IngestionLogSerializer

    def get_queryset(self):
        qs = IngestionLog.objects.order_by("-created_at")
        source = self.request.query_params.get("source")
        status = self.request.query_params.get("status")
        if source:
            qs = qs.filter(source__icontains=source)
        if status:
            qs = qs.filter(status=status)
        return qs


class IngestionLogDetailView(generics.RetrieveAPIView):
    queryset         = IngestionLog.objects.all()
    serializer_class = IngestionLogSerializer
