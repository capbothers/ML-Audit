"""
Helper utilities
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import hashlib
import json


def calculate_date_range(days: int = 30) -> tuple[datetime, datetime]:
    """Calculate date range for analysis"""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def hash_data(data: Any) -> str:
    """Create hash of data for caching/deduplication"""
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(data_str.encode()).hexdigest()


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers"""
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ZeroDivisionError):
        return default


def calculate_percentage_change(current: float, previous: float) -> Optional[float]:
    """Calculate percentage change between two values"""
    if previous == 0:
        return None
    return ((current - previous) / previous) * 100


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split list into chunks"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def extract_domain(url: str) -> str:
    """Extract domain from URL"""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.netloc or parsed.path


def format_currency(amount: float, currency: str = "USD") -> str:
    """Format amount as currency"""
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    symbol = symbols.get(currency, currency)
    return f"{symbol}{amount:,.2f}"
