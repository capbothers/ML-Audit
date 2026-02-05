#!/usr/bin/env python3
"""
GA4 Historical Backfill Script

Backfills GA4 data for 12-16 months in weekly chunks to avoid:
- API sampling (>500K events/day)
- Memory issues (10K+ rows per request)
- Rate limiting (10K requests/day)

Usage:
    python scripts/backfill_ga4.py [--months 14] [--chunk-days 7] [--tables all]

Examples:
    # Full backfill (14 months, all tables)
    python scripts/backfill_ga4.py

    # Backfill only daily summary for 6 months
    python scripts/backfill_ga4.py --months 6 --tables summary

    # Backfill geo data with smaller chunks to avoid sampling
    python scripts/backfill_ga4.py --tables geo --chunk-days 3

Tables available:
    all, summary, device, geo, user_type, sources, pages, landing, products, events, ecommerce
"""
import asyncio
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytz
from app.connectors.ga4_connector import GA4Connector
from app.services.data_sync_service import DataSyncService
from app.utils.logger import log

SYDNEY_TZ = pytz.timezone('Australia/Sydney')


async def backfill_ga4(
    months: int = 14,
    chunk_days: int = 7,
    tables: str = "all",
    delay_between_chunks: float = 1.0,
    dry_run: bool = False,
    geo_granularity: str = "country"
):
    """
    Backfill GA4 data in chunks.

    Args:
        months: Number of months to backfill (max 16 for GA4 Data API)
        chunk_days: Days per chunk (smaller = less sampling risk)
        tables: Which tables to backfill ("all" or specific table name)
        delay_between_chunks: Seconds between chunks (rate limit protection)
        dry_run: If True, only fetch data without saving
        geo_granularity: Level of geo detail ("country", "region", or "city")
    """
    connector = GA4Connector()
    sync_service = DataSyncService()

    # Calculate date range
    now = datetime.now(SYDNEY_TZ)
    end_date = now - timedelta(days=2)  # GA4 has 48-hour delay
    start_date = end_date - timedelta(days=months * 30)

    print(f"\n{'='*60}")
    print(f"GA4 Historical Backfill")
    print(f"{'='*60}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Chunk size: {chunk_days} days")
    print(f"Tables: {tables}")
    print(f"Geo granularity: {geo_granularity}")
    print(f"Delay between chunks: {delay_between_chunks}s")
    if dry_run:
        print(f"MODE: DRY RUN (no data will be saved)")
    print(f"{'='*60}\n")

    # Connect to GA4
    print("Connecting to GA4...")
    if not await connector.connect():
        print("ERROR: Failed to connect to GA4. Check credentials.")
        return

    print("Connected successfully!\n")

    # Determine which fetch methods to use
    table_methods = {
        'summary': '_fetch_daily_summary',
        'device': '_fetch_device_breakdown',
        'geo': '_fetch_geo_breakdown',
        'user_type': '_fetch_user_type_breakdown',
        'sources': '_fetch_traffic_sources_paginated',
        'pages': '_fetch_page_performance_paginated',
        'landing': '_fetch_landing_pages_paginated',
        'products': '_fetch_product_performance_paginated',
        'events': '_fetch_conversions',
        'ecommerce': '_fetch_ecommerce_metrics',
    }

    if tables == 'all':
        selected_tables = list(table_methods.keys())
    else:
        selected_tables = [tables]

    print(f"Tables to backfill: {', '.join(selected_tables)}\n")

    # Process in chunks
    total_chunks = 0
    total_records = {t: 0 for t in selected_tables}
    current_start = start_date
    errors = []

    while current_start < end_date:
        chunk_end = min(current_start + timedelta(days=chunk_days - 1), end_date)
        total_chunks += 1

        print(f"\n--- Chunk {total_chunks}: {current_start.date()} to {chunk_end.date()} ---")

        try:
            # Fetch data for this chunk
            data = {}

            for table in selected_tables:
                method_name = table_methods.get(table)
                if not method_name:
                    continue

                method = getattr(connector, method_name)

                try:
                    if table == 'geo':
                        # Geo has special granularity parameter
                        result = await method(current_start, chunk_end, granularity=geo_granularity)
                    else:
                        result = await method(current_start, chunk_end)

                    # Map to the key expected by save logic
                    key_mapping = {
                        'summary': 'daily_summary',
                        'device': 'device_breakdown',
                        'geo': 'geo_breakdown',
                        'user_type': 'user_type_breakdown',
                        'sources': 'traffic_sources',
                        'pages': 'pages',
                        'landing': 'landing_pages',
                        'products': 'products',
                        'events': 'conversions',
                        'ecommerce': 'ecommerce',
                    }
                    data[key_mapping[table]] = result
                    record_count = len(result) if isinstance(result, list) else len(result.get('daily_metrics', []))
                    total_records[table] += record_count
                    print(f"  {table}: {record_count} records")

                except Exception as e:
                    print(f"  ERROR fetching {table}: {e}")
                    errors.append(f"Chunk {total_chunks} - {table}: {e}")

            # Save to database
            if not dry_run and data:
                try:
                    result = sync_service._save_ga4_data(data)
                    created = result.get('created', 0)
                    updated = result.get('updated', 0)
                    failed = result.get('failed', 0)
                    print(f"  SAVED: {created} created, {updated} updated, {failed} failed")
                except Exception as e:
                    print(f"  ERROR saving: {e}")
                    errors.append(f"Chunk {total_chunks} - save: {e}")

        except Exception as e:
            print(f"  CRITICAL ERROR: {e}")
            errors.append(f"Chunk {total_chunks} - critical: {e}")

        # Move to next chunk
        current_start = chunk_end + timedelta(days=1)

        # Rate limit delay
        if current_start < end_date:
            await asyncio.sleep(delay_between_chunks)

    # Summary
    print(f"\n{'='*60}")
    print(f"Backfill Complete!")
    print(f"{'='*60}")
    print(f"Total chunks processed: {total_chunks}")
    print(f"\nRecords fetched by table:")
    for table, count in total_records.items():
        print(f"  {table}: {count:,}")
    print(f"\nTotal records: {sum(total_records.values()):,}")

    if errors:
        print(f"\nErrors encountered ({len(errors)}):")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

    print(f"{'='*60}\n")


async def verify_backfill():
    """Verify backfill by checking record counts."""
    from app.models.base import SessionLocal
    from app.models.ga4_data import (
        GA4DailySummary, GA4DeviceBreakdown, GA4GeoBreakdown, GA4UserType,
        GA4TrafficSource, GA4PagePerformance, GA4LandingPage, GA4ProductPerformance,
        GA4Event, GA4DailyEcommerce
    )
    from sqlalchemy import func

    db = SessionLocal()

    print("\n--- GA4 Data Verification ---\n")

    tables = [
        ("ga4_daily_summary", GA4DailySummary),
        ("ga4_device_breakdown", GA4DeviceBreakdown),
        ("ga4_geo_breakdown", GA4GeoBreakdown),
        ("ga4_user_type", GA4UserType),
        ("ga4_traffic_sources", GA4TrafficSource),
        ("ga4_pages", GA4PagePerformance),
        ("ga4_landing_pages", GA4LandingPage),
        ("ga4_products", GA4ProductPerformance),
        ("ga4_events", GA4Event),
        ("ga4_daily_ecommerce", GA4DailyEcommerce),
    ]

    for table_name, model in tables:
        try:
            count = db.query(func.count(model.id)).scalar() or 0
            min_date = db.query(func.min(model.date)).scalar()
            max_date = db.query(func.max(model.date)).scalar()

            if count > 0:
                print(f"{table_name}:")
                print(f"  Records: {count:,}")
                print(f"  Date range: {min_date} to {max_date}")
            else:
                print(f"{table_name}: EMPTY")
        except Exception as e:
            print(f"{table_name}: ERROR - {e}")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill GA4 historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tables available:
  all      - All tables (default)
  summary  - Daily site-wide metrics (ga4_daily_summary)
  device   - Device breakdown (ga4_device_breakdown)
  geo      - Geographic breakdown (ga4_geo_breakdown)
  user_type - New vs returning users (ga4_user_type)
  sources  - Traffic sources (ga4_traffic_sources)
  pages    - Page performance (ga4_pages)
  landing  - Landing pages (ga4_landing_pages)
  products - Product performance (ga4_products)
  events   - Events/conversions (ga4_events)
  ecommerce - Daily ecommerce totals (ga4_daily_ecommerce)

Geo granularity options:
  country  - Country-level only (default, fastest)
  region   - Country + region/state (more rows)
  city     - Country + region + city (most rows, use smaller chunks)

Examples:
  # Full 14-month backfill
  python scripts/backfill_ga4.py

  # Only backfill new tables (Phase 2)
  python scripts/backfill_ga4.py --tables summary
  python scripts/backfill_ga4.py --tables device
  python scripts/backfill_ga4.py --tables geo
  python scripts/backfill_ga4.py --tables user_type

  # Backfill geo with region-level detail
  python scripts/backfill_ga4.py --tables geo --geo-granularity region

  # Backfill geo with city-level detail (use smaller chunks)
  python scripts/backfill_ga4.py --tables geo --geo-granularity city --chunk-days 3

  # Verify existing data
  python scripts/backfill_ga4.py --verify
"""
    )
    parser.add_argument(
        "--months", type=int, default=14,
        help="Months to backfill (max 16 for GA4, default: 14)"
    )
    parser.add_argument(
        "--chunk-days", type=int, default=7,
        help="Days per chunk (default: 7)"
    )
    parser.add_argument(
        "--tables", type=str, default="all",
        choices=["all", "summary", "device", "geo", "user_type", "sources",
                 "pages", "landing", "products", "events", "ecommerce"],
        help="Which tables to backfill (default: all)"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds between chunks (default: 1.0)"
    )
    parser.add_argument(
        "--geo-granularity", type=str, default="country",
        choices=["country", "region", "city"],
        help="Geo detail level: country (default), region, or city"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch data without saving to database"
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Verify existing data (no backfill)"
    )

    args = parser.parse_args()

    if args.verify:
        asyncio.run(verify_backfill())
    else:
        asyncio.run(backfill_ga4(
            months=args.months,
            chunk_days=args.chunk_days,
            tables=args.tables,
            delay_between_chunks=args.delay,
            dry_run=args.dry_run,
            geo_granularity=args.geo_granularity
        ))
