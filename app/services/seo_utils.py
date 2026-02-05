"""
SEO Utility Functions

CTR curve lookup, URL classifier, and sparkline helpers.
"""
import json
import os
from functools import lru_cache
from typing import Dict, List, Optional
from urllib.parse import urlparse


STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "static")


@lru_cache(maxsize=1)
def load_ctr_curve() -> List[Dict]:
    path = os.path.join(STATIC_DIR, "ctr_curve.json")
    with open(path, "r") as f:
        data = json.load(f)
    return data["bands"]


@lru_cache(maxsize=1)
def load_url_rules() -> Dict:
    path = os.path.join(STATIC_DIR, "url_rules.json")
    with open(path, "r") as f:
        return json.load(f)


def expected_ctr_for_position(position: float) -> float:
    """Return expected CTR for a given average position using the CTR curve."""
    bands = load_ctr_curve()
    for band in bands:
        if band["min"] <= position < band["max"]:
            return band["expected_ctr"]
    return 0.005


def classify_url(url: str) -> Dict:
    """Classify a URL into category/template with effort weight.

    Returns: {"category": str, "template": str, "effort_weight": float}
    """
    rules = load_url_rules()
    if not url:
        return rules["default"]

    parsed = urlparse(url)
    path = parsed.path.lower()

    # Homepage check
    if path in ("", "/"):
        return rules["homepage"]

    # Pattern matching
    for pattern in rules["patterns"]:
        if pattern["match"] in path:
            return {
                "category": pattern["category"],
                "template": pattern["template"],
                "effort_weight": pattern["effort_weight"],
            }

    return rules["default"]


def shorten_url(url: str, domain: str = "https://www.cassbrothers.com.au") -> str:
    """Strip domain prefix for display."""
    if not url:
        return "-"
    return url.replace(domain, "") or "/"


def sparkline_points(values: List[float], width: int = 80, height: int = 20) -> str:
    """Generate SVG polyline points from a list of values."""
    if not values or len(values) < 2:
        return ""
    mn = min(values)
    mx = max(values)
    rng = mx - mn if mx != mn else 1
    pts = []
    step = width / (len(values) - 1)
    for i, v in enumerate(values):
        x = round(i * step, 1)
        y = round(height - ((v - mn) / rng) * height, 1)
        pts.append(f"{x},{y}")
    return " ".join(pts)
