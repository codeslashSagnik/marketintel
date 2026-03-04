"""
common/validators.py

Shared validation functions used across ingestion services and ETL pipelines.
Raise ValueError with clear messages so callers can log and skip bad records.
"""
import logging
from decimal import Decimal, InvalidOperation
from django.utils import timezone

logger = logging.getLogger("common.validators")


# ── Price validators ───────────────────────────────────────────────────────────

def validate_price(value, field_name: str = "price") -> Decimal:
    """
    Ensure a price value is a positive Decimal.

    Raises:
        ValueError: if the value is non-numeric or non-positive.
    """
    try:
        price = Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise ValueError(f"{field_name}='{value}' is not a valid decimal number")

    if price < 0:
        raise ValueError(f"{field_name}={price} must be >= 0")

    return price


def validate_sentiment_score(score) -> float:
    """
    Ensure sentiment score is in the range [-1.0, +1.0].

    Raises:
        ValueError: outside valid range.
    """
    try:
        score = float(score)
    except (TypeError, ValueError):
        raise ValueError(f"sentiment_score='{score}' is not a valid float")

    if not -1.0 <= score <= 1.0:
        raise ValueError(f"sentiment_score={score} must be in [-1.0, +1.0]")

    return score


# ── String validators ──────────────────────────────────────────────────────────

def validate_non_empty(value: str, field_name: str = "field") -> str:
    """
    Ensure a string value is non-empty after stripping whitespace.

    Raises:
        ValueError: if blank or None.
    """
    if not value or not str(value).strip():
        raise ValueError(f"{field_name} must not be empty")
    return str(value).strip()


def validate_sku(sku: str) -> str:
    """
    Minimal SKU format validation — alphanumeric + dashes/underscores.

    Raises:
        ValueError: if format invalid.
    """
    import re
    sku = validate_non_empty(sku, "sku_code")
    if not re.match(r"^[A-Za-z0-9_\-]+$", sku):
        raise ValueError(f"sku_code='{sku}' contains invalid characters")
    return sku.upper()


# ── Temporal validators ────────────────────────────────────────────────────────

def validate_not_future(dt, field_name: str = "timestamp"):
    """
    Ensure a datetime is not in the future (allow 60-second tolerance).

    Raises:
        ValueError: if the timestamp is more than 60s in the future.
    """
    from datetime import timedelta
    if dt > timezone.now() + timedelta(seconds=60):
        raise ValueError(f"{field_name}={dt} is in the future")
    return dt
