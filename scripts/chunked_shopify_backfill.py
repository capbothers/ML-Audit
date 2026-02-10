#!/usr/bin/env python3
"""
Chunked Shopify backfill — processes orders in small date ranges to avoid OOM.

Usage:
    python scripts/chunked_shopify_backfill.py [--gap-only] [--refunds-only] [--latest-only]

Default: runs all three stages:
  1. Fill the Jan 6-27 2025 gap (orders missing from previous sync)
  2. Fetch latest orders (Feb 5-10 2026)
  3. Fetch refund line items for all refunded/partially_refunded orders
"""
import asyncio
import argparse
import gc
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.base import SessionLocal
from app.connectors.shopify_connector import ShopifyConnector
from app.services.data_sync_service import DataSyncService
from app.utils.logger import log
from sqlalchemy import text
import pytz

SYDNEY_TZ = pytz.timezone("Australia/Sydney")


async def fetch_and_save_orders(service: DataSyncService, start: datetime, end: datetime, label: str):
    """Fetch orders for a date range and save them."""
    log.info(f"[{label}] Fetching orders from {start} to {end}")

    connector = service.shopify
    if not connector.session:
        await connector.connect()

    orders = await connector._fetch_orders(start, end)
    log.info(f"[{label}] Fetched {len(orders)} orders")

    if not orders:
        return 0

    # Wrap in expected format and save
    data = {
        'orders': {
            'items': orders,
            'total_orders': len(orders),
        }
    }
    result = service._save_shopify_orders(data)
    log.info(f"[{label}] Saved: {result['created']} created, {result['updated']} updated, {result['failed']} failed")

    # Free memory
    del orders
    del data
    gc.collect()

    return result['created'] + result['updated']


async def fetch_and_save_refunds(service: DataSyncService, order_ids: list, batch_size: int = 30):
    """Fetch refunds in batches and save them."""
    connector = service.shopify
    if not connector.session:
        await connector.connect()

    total_saved = 0
    num_batches = (len(order_ids) + batch_size - 1) // batch_size

    for i in range(0, len(order_ids), batch_size):
        batch = order_ids[i:i + batch_size]
        batch_num = i // batch_size + 1
        label = f"Refund batch {batch_num}/{num_batches}"
        log.info(f"[{label}] Fetching refunds for {len(batch)} orders")

        refund_items = await connector._fetch_refunds(batch)

        if refund_items:
            data = {'refunds': {'items': refund_items}}
            result = service._save_shopify_refunds(data)
            saved = result['created'] + result['updated']
            total_saved += saved
            log.info(f"[{label}] Saved {saved} refunds ({result['created']} new, {result['updated']} updated)")
        else:
            log.info(f"[{label}] No refunds found for this batch")

        # Free memory between batches
        del refund_items
        gc.collect()

        # Small delay between batches for rate limiting
        await asyncio.sleep(0.5)

    return total_saved


async def stage_gap_fill(service: DataSyncService):
    """Stage 1: Fill the Jan 6-27 2025 gap."""
    print("\n=== Stage 1: Fill Jan 6-27 2025 order gap ===")
    start = SYDNEY_TZ.localize(datetime(2025, 1, 6, 0, 0, 0))
    end = SYDNEY_TZ.localize(datetime(2025, 1, 28, 0, 0, 0))
    saved = await fetch_and_save_orders(service, start, end, "Gap fill")
    print(f"Gap fill complete: {saved} orders processed")
    return saved


async def stage_latest(service: DataSyncService):
    """Stage 2: Fetch latest orders since last sync."""
    print("\n=== Stage 2: Fetch latest orders ===")
    db = SessionLocal()
    try:
        row = db.execute(text("SELECT MAX(created_at) FROM shopify_orders")).fetchone()
        max_date_str = row[0]
    finally:
        db.close()

    if max_date_str:
        if isinstance(max_date_str, str):
            # Parse various datetime formats
            for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                try:
                    max_date = datetime.strptime(max_date_str.split('+')[0], fmt)
                    break
                except ValueError:
                    continue
            else:
                max_date = datetime.now() - timedelta(days=5)
        else:
            max_date = max_date_str
        start = SYDNEY_TZ.localize(datetime(max_date.year, max_date.month, max_date.day) - timedelta(days=1))
    else:
        start = SYDNEY_TZ.localize(datetime(2025, 1, 6))

    now = datetime.now()
    end = SYDNEY_TZ.localize(datetime(now.year, now.month, now.day, 23, 59, 59))
    saved = await fetch_and_save_orders(service, start, end, "Latest")
    print(f"Latest sync complete: {saved} orders processed")
    return saved


async def stage_refunds(service: DataSyncService):
    """Stage 3: Fetch refund line items for all refunded orders."""
    print("\n=== Stage 3: Fetch refund line items ===")
    db = SessionLocal()
    try:
        rows = db.execute(text(
            "SELECT shopify_order_id FROM shopify_orders "
            "WHERE financial_status IN ('refunded', 'partially_refunded') "
            "ORDER BY shopify_order_id"
        )).fetchall()
        order_ids = [r[0] for r in rows]

        existing = db.execute(text("SELECT COUNT(*) FROM shopify_refund_line_items")).fetchone()[0]
    finally:
        db.close()

    print(f"Found {len(order_ids)} refunded orders, {existing} existing refund line items")

    if not order_ids:
        print("No refunded orders found — skipping")
        return 0

    saved = await fetch_and_save_refunds(service, order_ids, batch_size=30)
    print(f"Refund sync complete: {saved} refunds saved")

    # Verify
    db = SessionLocal()
    try:
        final_count = db.execute(text("SELECT COUNT(*) FROM shopify_refund_line_items")).fetchone()[0]
    finally:
        db.close()

    print(f"Total refund line items in DB: {final_count}")
    return saved


async def main():
    parser = argparse.ArgumentParser(description="Chunked Shopify backfill")
    parser.add_argument("--gap-only", action="store_true", help="Only fill the Jan 6-27 gap")
    parser.add_argument("--latest-only", action="store_true", help="Only fetch latest orders")
    parser.add_argument("--refunds-only", action="store_true", help="Only fetch refund line items")
    args = parser.parse_args()

    run_all = not (args.gap_only or args.latest_only or args.refunds_only)

    service = DataSyncService()

    if run_all or args.gap_only:
        await stage_gap_fill(service)

    if run_all or args.latest_only:
        await stage_latest(service)

    if run_all or args.refunds_only:
        await stage_refunds(service)

    print("\n=== Done ===")
    db = SessionLocal()
    try:
        for table in ['shopify_orders', 'shopify_order_items', 'shopify_refund_line_items']:
            row = db.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            print(f"  {table}: {row[0]:,} rows")
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(main())
