"""Simple in-memory TTL cache for expensive dashboard queries."""
import time
from typing import Any

_cache: dict[str, tuple[float, Any]] = {}
_MISS = object()


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
    """Clear all cached values (call after data sync)."""
    _cache.clear()
