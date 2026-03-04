"""
common/utils.py

Shared utility functions used across the entire market_intelligence project.
"""
import logging
import time
from functools import wraps
from typing import Any

logger = logging.getLogger("common.utils")


# ── Type-safe coercions ────────────────────────────────────────────────────────

def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float.
    Returns ``default`` if conversion fails.

    >>> safe_float("12.5")
    12.5
    >>> safe_float(None)
    0.0
    >>> safe_float("N/A")
    0.0
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to int.
    Returns ``default`` if conversion fails.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ── Timing helpers ─────────────────────────────────────────────────────────────

def measure_time(func):
    """
    Decorator that logs execution time of the wrapped function.
    Compatible with both sync and async functions (sync only here).

    Usage:
        @measure_time
        def my_service_call():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        try:
            result = func(*args, **kwargs)
            duration = time.monotonic() - start
            logger.info(
                "function=%s duration=%.3fs status=success",
                func.__qualname__, duration,
            )
            return result
        except Exception as exc:
            duration = time.monotonic() - start
            logger.error(
                "function=%s duration=%.3fs status=failed error=%s",
                func.__qualname__, duration, exc,
            )
            raise
    return wrapper


# ── Data cleaning helpers ──────────────────────────────────────────────────────

def clean_price_string(price_str: str) -> float:
    """
    Strip currency symbols, commas and whitespace from a price string,
    then return a float.

    >>> clean_price_string("₹ 1,299.00")
    1299.0
    >>> clean_price_string("Rs.450")
    450.0
    """
    if not price_str:
        return 0.0
    cleaned = (
        str(price_str)
        .replace("₹", "")
        .replace("Rs.", "")
        .replace("Rs", "")
        .replace(",", "")
        .strip()
    )
    return safe_float(cleaned)


def chunks(lst: list, n: int):
    """
    Yield successive n-sized chunks from lst.
    Useful for batching bulk_create calls.

    >>> list(chunks([1,2,3,4,5], 2))
    [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def truncate_text(text: str, max_length: int = 2000) -> str:
    """Truncate text to ``max_length`` chars, appending '...' if cut."""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."
