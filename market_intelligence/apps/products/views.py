"""
apps/products/views.py
"""
import logging
from rest_framework import generics
from .models import Product
from .serializers import ProductSerializer

logger = logging.getLogger(__name__)


class ProductListView(generics.ListCreateAPIView):
    queryset         = Product.objects.filter(is_active=True)
    serializer_class = ProductSerializer


class ProductDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset         = Product.objects.all()
    serializer_class = ProductSerializer
