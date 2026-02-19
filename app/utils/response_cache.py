"""Simple TTL response cache for expensive dashboard endpoints.

Usage:
    from app.utils.response_cache import response_cache

    @router.get("/heavy-endpoint")
    async def heavy_endpoint(days: int = 30, db: Session = Depends(get_db)):
        cache_key = f"heavy:{days}"
        cached = response_cache.get(cache_key)
        if cached:
            return cached
        # ... expensive computation ...
        result = {"success": True, "data": ...}
        response_cache.set(cache_key, result, ttl=300)
        return result
"""
import threading
import time
from typing import Any


class ResponseCache:
    """Thread-safe in-memory cache with TTL expiry and max-entry limit."""

    def __init__(self, max_entries: int = 80):
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._max_entries = max_entries

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.time() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        with self._lock:
            # Evict expired entries first to stay under limit
            if len(self._store) >= self._max_entries:
                now = time.time()
                expired = [k for k, (exp, _) in self._store.items() if now > exp]
                for k in expired:
                    del self._store[k]
            # If still at limit, evict oldest entry
            if len(self._store) >= self._max_entries:
                oldest_key = min(self._store, key=lambda k: self._store[k][0])
                del self._store[oldest_key]
            self._store[key] = (time.time() + ttl, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def invalidate(self, prefix: str) -> int:
        """Remove all keys starting with prefix. Returns count removed."""
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            return len(keys)


response_cache = ResponseCache()
