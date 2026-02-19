"""Simple in-memory TTL cache for expensive dashboard queries."""
import time
from typing import Any

_cache: dict[str, tuple[float, Any]] = {}
_MISS = object()

# Which cache key prefixes depend on which data source.
# Used by clear_for_source() so a Shopify sync doesn't nuke SEO caches etc.
_SOURCE_PREFIXES: dict[str, list[str]] = {
    "shopify": [
        "finance_", "perf_summary", "ml_drivers", "ml_tracking",
        "ml_forecast", "ml_anomalies", "ml_inventory", "ml_inv_dashboard",
        "ml_stock_health", "pricing_unmatchable", "pricing_brand_summary",
    ],
    "ga4": ["perf_summary", "ml_drivers", "ml_tracking"],
    "search_console": ["seo_"],
    "google_ads": ["ads:"],
    "cost_sheet": ["pricing_", "finance_"],
    "merchant_center": [],
}


def get_cached(key: str):
    """Return cached value if still valid, else _MISS sentinel."""
    now = time.time()
    if key in _cache:
        expires, value = _cache[key]
        if now < expires:
            return value
    return _MISS


def set_cached(key: str, value: Any, seconds: int = 300):
    """Store a value in cache with TTL."""
    _cache[key] = (time.time() + seconds, value)


def clear_cache():
    """Clear all cached values (call after /sync/all or full sync)."""
    _cache.clear()


def clear_for_source(source: str):
    """Clear only cache entries affected by a specific data source sync."""
    prefixes = _SOURCE_PREFIXES.get(source)
    if prefixes is None:
        # Unknown source — fall back to clearing everything
        _cache.clear()
        return
    keys_to_remove = [
        k for k in _cache
        if any(k.startswith(p) for p in prefixes)
    ]
    for k in keys_to_remove:
        del _cache[k]
