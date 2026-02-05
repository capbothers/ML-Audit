#!/usr/bin/env python3
"""
Data Freshness Audit Script

Prints max dates and row counts for every key data table.
Flags any source that is stale (behind its expected threshold).

Usage:
    python scripts/audit_freshness.py
    python scripts/audit_freshness.py --json   # machine-readable output
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from sqlalchemy import func
from app.models.base import SessionLocal, init_db
from app.models.shopify import ShopifyOrder, ShopifyOrderItem
from app.models.ga4_data import GA4DailySummary, GA4TrafficSource
from app.models.search_console_data import SearchConsoleQuery, SearchConsolePage
from app.models.google_ads_data import GoogleAdsCampaign, GoogleAdsAdGroup, GoogleAdsSearchTerm
from app.models.merchant_center_data import MerchantCenterProductStatus
from app.models.data_quality import DataSyncStatus
from app.config import get_settings


# Base checks always run
_BASE_CHECKS = [
    ("shopify_orders",          ShopifyOrder,             ShopifyOrder.created_at,                6),
    ("shopify_order_items",     ShopifyOrderItem,         ShopifyOrderItem.order_date,            6),
    ("ga4_daily_summary",       GA4DailySummary,          GA4DailySummary.date,                  72),
    ("ga4_traffic_sources",     GA4TrafficSource,         GA4TrafficSource.date,                 72),
    ("search_console_queries",  SearchConsoleQuery,       SearchConsoleQuery.date,               96),
    ("search_console_pages",    SearchConsolePage,        SearchConsolePage.date,                96),
    ("google_ads_campaigns",    GoogleAdsCampaign,        GoogleAdsCampaign.date,                48),
    ("merchant_center",         MerchantCenterProductStatus, MerchantCenterProductStatus.snapshot_date, 48),
]

# Only checked when NOT in Sheets-only mode (direct Google Ads API)
_API_ONLY_CHECKS = [
    ("google_ads_ad_groups",    GoogleAdsAdGroup,         GoogleAdsAdGroup.date,                 48),
    ("google_ads_search_terms", GoogleAdsSearchTerm,      GoogleAdsSearchTerm.date,              48),
]


def _build_checks():
    settings = get_settings()
    checks = list(_BASE_CHECKS)
    if not settings.google_ads_sheet_id:
        checks.extend(_API_ONLY_CHECKS)
    return checks


CHECKS = _build_checks()


def run_audit(as_json=False):
    init_db()
    db = SessionLocal()
    now = datetime.utcnow()
    results = []

    try:
        for label, model, date_col, threshold_h in CHECKS:
            try:
                max_date = db.query(func.max(date_col)).scalar()
                row_count = db.query(func.count(model.id)).scalar() or 0
            except Exception:
                max_date = None
                row_count = 0

            if max_date is None:
                lag_hours = None
                status = "EMPTY"
            else:
                if not isinstance(max_date, datetime):
                    max_date = datetime.combine(max_date, datetime.min.time())
                lag_hours = round((now - max_date).total_seconds() / 3600, 1)
                status = "STALE" if lag_hours > threshold_h else "OK"

            results.append({
                "source": label,
                "rows": row_count,
                "max_date": max_date.isoformat() if max_date else None,
                "lag_hours": lag_hours,
                "threshold_hours": threshold_h,
                "status": status,
            })

        # Also print data_sync_status table contents
        sync_statuses = db.query(DataSyncStatus).all()

    finally:
        db.close()

    if as_json:
        import json
        stale = [r for r in results if r["status"] != "OK"]
        print(json.dumps({
            "checked_at": now.isoformat(),
            "all_fresh": len(stale) == 0,
            "stale_count": len(stale),
            "sources": results,
            "data_sync_status_rows": len(sync_statuses),
        }, indent=2))
        return

    # Pretty-print
    print(f"\n{'='*80}")
    print(f"  DATA FRESHNESS AUDIT  —  {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*80}\n")

    header = f"{'Source':<30} {'Rows':>8} {'Max Date':>20} {'Lag (h)':>8} {'Thresh':>7} {'Status':>7}"
    print(header)
    print("-" * len(header))

    stale_count = 0
    for r in results:
        status_str = r["status"]
        lag_str = f"{r['lag_hours']:.1f}" if r["lag_hours"] is not None else "N/A"
        date_str = r["max_date"][:19] if r["max_date"] else "—"

        marker = ""
        if status_str == "STALE":
            marker = " <<"
            stale_count += 1
        elif status_str == "EMPTY":
            marker = " !!"
            stale_count += 1

        print(f"{r['source']:<30} {r['rows']:>8} {date_str:>20} {lag_str:>8} {r['threshold_hours']:>7} {status_str:>7}{marker}")

    print(f"\n{'='*80}")
    if stale_count == 0:
        print("  All sources are fresh.")
    else:
        print(f"  {stale_count} source(s) are STALE or EMPTY.")
    print(f"{'='*80}")

    # data_sync_status summary
    print(f"\n  data_sync_status table: {len(sync_statuses)} rows")
    if sync_statuses:
        for s in sync_statuses:
            last_ok = s.last_successful_sync.strftime('%Y-%m-%d %H:%M') if s.last_successful_sync else "never"
            print(f"    {s.source_name:<20} status={s.sync_status:<8} last_ok={last_ok}  health={s.health_score}")

    print()


if __name__ == "__main__":
    as_json = "--json" in sys.argv
    run_audit(as_json=as_json)
