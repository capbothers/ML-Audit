#!/usr/bin/env python3
"""
Re-sync refunds from Shopify — fixes missing refund data.

Problem: Our order sync fetches by created_at, so orders that were later
refunded still show financial_status='paid' in our DB. We only fetched
refunds for orders with refunded/partially_refunded status, missing thousands.

Fix: Query Shopify for ALL orders with refund-related financial_status,
update our DB, then fetch refunds for any orders missing refund records.

Usage:
    python scripts/resync_refunds.py [--dry-run] [--batch-size 50]
"""
import asyncio
import argparse
import gc
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shopify
from app.models.base import SessionLocal
from app.connectors.shopify_connector import ShopifyConnector
from app.services.data_sync_service import DataSyncService
from app.utils.logger import log
from sqlalchemy import text


async def fetch_refunded_order_ids_from_shopify(connector: ShopifyConnector):
    """Fetch ALL order IDs with refund status from Shopify API (lightweight)."""
    if not connector.session:
        await connector.connect()

    all_orders = []

    for status in ['partially_refunded', 'refunded']:
        print(f"\nFetching orders with financial_status={status}...")
        page = 1
        orders = shopify.Order.find(
            status='any',
            financial_status=status,
            limit=250,
        )

        while orders:
            page_count = len(orders)
            print(f"  Page {page}: {page_count} orders")

            for order in orders:
                all_orders.append({
                    'id': order.id,
                    'financial_status': order.financial_status,
                    'updated_at': order.updated_at,
                })

            if orders.has_next_page():
                orders = orders.next_page()
                page += 1
            else:
                break

        print(f"  Total {status}: {len([o for o in all_orders if o['financial_status'] == status])}")

    print(f"\nTotal orders with refund status in Shopify: {len(all_orders)}")
    return all_orders


async def fetch_full_orders_from_shopify(connector: ShopifyConnector, order_ids: list, batch_size: int = 50):
    """Fetch full order data (with line_items) for specific order IDs."""
    if not connector.session:
        await connector.connect()

    all_orders = []
    num_batches = (len(order_ids) + batch_size - 1) // batch_size

    for i in range(0, len(order_ids), batch_size):
        batch = order_ids[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"  Fetching full order data batch {batch_num}/{num_batches} ({len(batch)} orders)")

        for oid in batch:
            try:
                order = shopify.Order.find(oid)
                order_data = {
                    "id": order.id,
                    "order_number": order.order_number,
                    "email": order.email,
                    "total_price": float(order.total_price) if order.total_price else 0,
                    "current_total_price": float(order.current_total_price) if hasattr(order, 'current_total_price') and order.current_total_price else float(order.total_price) if order.total_price else 0,
                    "subtotal_price": float(order.subtotal_price) if order.subtotal_price else 0,
                    "current_subtotal_price": float(order.current_subtotal_price) if hasattr(order, 'current_subtotal_price') and order.current_subtotal_price else None,
                    "total_tax": float(order.total_tax) if order.total_tax else 0,
                    "total_discounts": float(order.total_discounts) if order.total_discounts else 0,
                    "total_shipping": 0.0,
                    "currency": order.currency,
                    "financial_status": order.financial_status,
                    "fulfillment_status": order.fulfillment_status,
                    "created_at": order.created_at,
                    "updated_at": order.updated_at,
                    "processed_at": order.processed_at,
                    "cancelled_at": getattr(order, 'cancelled_at', None),
                    "cancel_reason": getattr(order, 'cancel_reason', None),
                    "customer_id": order.customer.id if order.customer else None,
                    "line_items_count": len(order.line_items) if order.line_items else 0,
                    "line_items": connector._extract_line_items(order.line_items) if order.line_items else [],
                    "source_name": order.source_name,
                    "referring_site": order.referring_site,
                    "landing_site": order.landing_site,
                    "tags": order.tags,
                    "note": order.note,
                    "gateway": getattr(order, 'gateway', None),
                }
                all_orders.append(order_data)
            except Exception as e:
                log.warning(f"Error fetching order {oid}: {e}")

        # Rate limit
        if i + batch_size < len(order_ids):
            await asyncio.sleep(1)
        gc.collect()

    return all_orders


async def main():
    parser = argparse.ArgumentParser(description="Re-sync refunds from Shopify")
    parser.add_argument("--dry-run", action="store_true", help="Only report gaps, don't fix")
    parser.add_argument("--batch-size", type=int, default=50, help="Orders per refund batch (default 50)")
    args = parser.parse_args()

    print("=" * 60)
    print("Shopify Refund Re-sync")
    print("=" * 60)

    connector = ShopifyConnector()
    service = DataSyncService()

    # Step 1: Get all refund-status orders from Shopify
    shopify_orders = await fetch_refunded_order_ids_from_shopify(connector)
    shopify_ids = {o['id'] for o in shopify_orders}

    # Step 2: Check which orders in our DB have stale financial_status
    db = SessionLocal()
    try:
        # Get current DB statuses for these orders
        rows = db.execute(text(
            "SELECT shopify_order_id, financial_status FROM shopify_orders"
        )).fetchall()
        db_status = {int(r[0]): r[1] for r in rows}

        # Find orders that exist in our DB but have wrong status
        stale_orders = []
        missing_orders = []
        already_correct = []

        for o in shopify_orders:
            oid = o['id']
            if oid in db_status:
                if db_status[oid] != o['financial_status']:
                    stale_orders.append(o)
                else:
                    already_correct.append(o)
            else:
                missing_orders.append(o)

        print(f"\n=== Status Analysis ===")
        print(f"Shopify refunded/partially_refunded orders: {len(shopify_orders)}")
        print(f"Already correct in our DB: {len(already_correct)}")
        print(f"Stale status (needs update): {len(stale_orders)}")
        print(f"Not in our DB at all: {len(missing_orders)}")

        # Show what statuses the stale orders currently have
        if stale_orders:
            stale_statuses = {}
            for o in stale_orders:
                current = db_status.get(o['id'], 'unknown')
                key = f"{current} -> {o['financial_status']}"
                stale_statuses[key] = stale_statuses.get(key, 0) + 1
            print(f"\nStale status transitions:")
            for k, v in sorted(stale_statuses.items()):
                print(f"  {k}: {v} orders")

        # Step 3: Check which orders are missing refund records
        existing_refund_orders = set()
        rows = db.execute(text(
            "SELECT DISTINCT shopify_order_id FROM shopify_refunds"
        )).fetchall()
        existing_refund_orders = {int(r[0]) for r in rows}

        orders_needing_refunds = []
        for o in shopify_orders:
            if o['id'] in db_status and o['id'] not in existing_refund_orders:
                orders_needing_refunds.append(o['id'])

        # Also re-fetch for stale orders (might have new refunds)
        for o in stale_orders:
            if o['id'] not in orders_needing_refunds:
                orders_needing_refunds.append(o['id'])

        print(f"\nOrders needing refund fetch: {len(orders_needing_refunds)}")
        print(f"Orders already have refund records: {len(existing_refund_orders)}")
    finally:
        db.close()

    if args.dry_run:
        print("\n[DRY RUN] Would update statuses, fetch missing orders, and fetch refunds. Exiting.")
        return

    # Step 4: Fetch full order data for missing orders
    if missing_orders:
        missing_ids = [o['id'] for o in missing_orders]
        print(f"\n=== Fetching {len(missing_ids)} missing orders from Shopify ===")
        full_orders = await fetch_full_orders_from_shopify(connector, missing_ids, batch_size=args.batch_size)

        if full_orders:
            print(f"Fetched {len(full_orders)} orders, saving to DB...")
            data = {
                'orders': {
                    'items': full_orders,
                    'total_orders': len(full_orders),
                }
            }
            result = service._save_shopify_orders(data)
            print(f"Saved: {result['created']} created, {result['updated']} updated, {result['failed']} failed")

            # These orders are now in DB and need refunds
            for oid in [o['id'] for o in full_orders]:
                if oid not in orders_needing_refunds:
                    orders_needing_refunds.append(oid)

            del full_orders
            gc.collect()

    # Step 5: Update stale financial_status in orders and order_items
    if stale_orders:
        print(f"\n=== Updating {len(stale_orders)} stale order statuses ===")
        db = SessionLocal()
        try:
            for o in stale_orders:
                db.execute(text(
                    "UPDATE shopify_orders SET financial_status = :status WHERE shopify_order_id = :oid"
                ), {"status": o['financial_status'], "oid": o['id']})
                db.execute(text(
                    "UPDATE shopify_order_items SET financial_status = :status WHERE shopify_order_id = :oid"
                ), {"status": o['financial_status'], "oid": o['id']})
            db.commit()
            print(f"Updated {len(stale_orders)} orders + their order items")
        except Exception as e:
            db.rollback()
            print(f"ERROR updating statuses: {e}")
        finally:
            db.close()

    # Step 6: Fetch refunds for orders that need them
    if orders_needing_refunds:
        print(f"\n=== Fetching refunds for {len(orders_needing_refunds)} orders ===")
        batch_size = args.batch_size
        total_saved = 0
        num_batches = (len(orders_needing_refunds) + batch_size - 1) // batch_size

        for i in range(0, len(orders_needing_refunds), batch_size):
            batch = orders_needing_refunds[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"\n  Batch {batch_num}/{num_batches}: {len(batch)} orders")

            refund_items = await connector._fetch_refunds(batch)

            if refund_items:
                data = {'refunds': {'items': refund_items}}
                result = service._save_shopify_refunds(data)
                saved = result['created'] + result['updated']
                total_saved += saved
                print(f"  Saved {saved} refunds ({result['created']} new, {result['updated']} updated)")
            else:
                print(f"  No refunds found")

            del refund_items
            gc.collect()
            await asyncio.sleep(0.5)

        print(f"\nTotal refunds saved: {total_saved}")

    # Step 6: Verify
    print(f"\n=== Verification ===")
    db = SessionLocal()
    try:
        r = db.execute(text("SELECT COUNT(DISTINCT shopify_order_id) FROM shopify_refunds")).fetchone()
        print(f"Orders with refund records: {r[0]}")

        r = db.execute(text("SELECT COUNT(*) FROM shopify_refund_line_items")).fetchone()
        print(f"Refund line items: {r[0]}")

        r = db.execute(text("""
            SELECT financial_status, COUNT(*)
            FROM shopify_orders
            GROUP BY financial_status
        """)).fetchall()
        print(f"\nOrder status distribution:")
        for row in r:
            print(f"  {row[0]}: {row[1]}")

        # Check Zip specifically
        r = db.execute(text("""
            SELECT SUM(rli.subtotal), COUNT(*)
            FROM shopify_refund_line_items rli
            JOIN shopify_order_items oi ON oi.line_item_id = rli.line_item_id
            WHERE oi.vendor = 'Zip'
        """)).fetchone()
        print(f"\nZip refund line items: {r[1]}, total value: ${float(r[0] or 0):,.2f}")

        # Check Zip refunds in Jan 2026 specifically
        r = db.execute(text("""
            SELECT SUM(rli.subtotal), COUNT(*)
            FROM shopify_refund_line_items rli
            JOIN shopify_refunds rf ON rf.shopify_refund_id = rli.shopify_refund_id
            JOIN shopify_order_items oi ON oi.line_item_id = rli.line_item_id
            WHERE oi.vendor = 'Zip'
              AND rf.created_at >= '2026-01-01' AND rf.created_at < '2026-02-01'
        """)).fetchone()
        print(f"Zip refunds processed in Jan 2026: {r[1]} items, ${float(r[0] or 0):,.2f}")
        print(f"Shopify reports: $19,868.19")

    finally:
        db.close()

    print(f"\n{'=' * 60}")
    print("Done!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
