"""
URL parsing utilities for extracting attribution parameters from landing site URLs.
"""
from urllib.parse import urlparse, parse_qs, unquote
from typing import Dict, Optional


def parse_landing_site(url: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Parse a Shopify landing_site URL and extract attribution parameters.

    Returns dict with keys:
        utm_source, utm_medium, utm_campaign, utm_term, utm_content,
        gclid, gad_campaign_id
    All values are None if not found.
    """
    result = {
        "utm_source": None,
        "utm_medium": None,
        "utm_campaign": None,
        "utm_term": None,
        "utm_content": None,
        "gclid": None,
        "gad_campaign_id": None,
    }

    if not url:
        return result

    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        result["utm_source"] = _first(params, "utm_source")
        result["utm_medium"] = _first(params, "utm_medium")
        result["utm_campaign"] = _first(params, "utm_campaign")
        result["utm_term"] = _first(params, "utm_term")
        result["utm_content"] = _first(params, "utm_content")
        result["gclid"] = _first(params, "gclid")
        result["gad_campaign_id"] = _first(params, "gad_campaignid")
    except Exception:
        pass

    return result


def _first(params: dict, key: str) -> Optional[str]:
    """Get first value for a query parameter, or None."""
    values = params.get(key)
    if values and values[0]:
        return unquote(values[0])
    return None
