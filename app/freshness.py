"""
Centralised source freshness thresholds and key normalisation.

All services that evaluate data staleness should import from here.
The canonical source keys match what track_sync() writes to data_sync_status.
"""
from datetime import datetime, timezone

# Canonical DB source keys → stale threshold in hours
# Must match the source names passed to track_sync() in data_sync_service.py
STALE_THRESHOLDS: dict[str, int] = {
    'shopify':              6,
    'ga4':                  72,   # GA4 has a ~48-72h processing delay
    'search_console':       96,   # Search Console has a ~3-day delay
    'google_ads':           48,
    'merchant_center':      48,
    'competitive_pricing':  168,  # Caprice — weekly cadence is normal
    'cost_sheet':           720,  # NETT Master Sheet — monthly cadence is normal
}

# Non-canonical key → canonical data_sync_status key
# Covers legacy names, LLM-generated names, and module dependency names
_ALIASES: dict[str, str] = {
    'google_sheets_costs':      'cost_sheet',
    'product_costs':            'cost_sheet',
    'shopify_orders':           'shopify',
    'search_console_queries':   'search_console',
    'search_console_pages':     'search_console',
}


def normalize_key(source: str) -> str:
    """Return the canonical data_sync_status key for any source name."""
    return _ALIASES.get(source, source)


def get_threshold(source: str) -> int:
    """Return the stale threshold in hours for a source (default 24h)."""
    return STALE_THRESHOLDS.get(normalize_key(source), 24)


def is_stale(source: str, last_sync: datetime | None) -> bool:
    """Return True if the source has not synced within its threshold."""
    if last_sync is None:
        return True
    threshold_h = get_threshold(source)
    now = datetime.now(timezone.utc)
    if last_sync.tzinfo is None:
        last_sync = last_sync.replace(tzinfo=timezone.utc)
    return (now - last_sync).total_seconds() / 3600 > threshold_h
