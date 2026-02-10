#!/usr/bin/env python3
"""
Re-sync ALL orders updated in January 2026 from Shopify.

This captures any order that had a refund processed in January 2026,
regardless of when the order was originally created. We update the
order's financial_status and current_total_price, then fetch all
refunds for orders that have refund-related status.

Usage:
    python scripts/resync_jan2026_refunds.py [--dry-run]
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


async def fetch_orders_updated_in_period(connector, start_date, end_date):
    """Fetch ALL orders updated in a date range from Shopify."""
    if not connector.session:
        await connector.connect()

    all_orders = []
    page = 1

    print(f"Fetching orders updated between {start_date} and {end_date}...")

    orders = shopify.Order.find(
        status='any',
        updated_at_min=start_date,
        updated_at_max=end_date,
        limit=250,
    )

    while orders:
        page_count = len(orders)
        print(f"  Page {page}: {page_count} orders")

        for order in orders:
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

        if orders.has_next_page():
            orders = orders.next_page()
            page += 1
        else:
            break

        # Periodically GC for large datasets
        if page % 10 == 0:
            gc.collect()

    print(f"Total orders updated in period: {len(all_orders)}")
    return all_orders


async def main():
    parser = argparse.ArgumentParser(description="Re-sync orders updated in Jan 2026")
    parser.add_argument("--dry-run", action="store_true", help="Only report, don't save")
    args = parser.parse_args()

    print("=" * 60)
    print("Re-sync Orders Updated in January 2026")
    print("=" * 60)

    connector = ShopifyConnector()
    service = DataSyncService()
    await connector.connect()

    # Step 1: Fetch all orders updated in January 2026
    orders = await fetch_orders_updated_in_period(
        connector,
        "2026-01-01T00:00:00+11:00",
        "2026-02-01T00:00:00+11:00"
    )

    # Analyze what we got
    status_counts = {}
    refund_statuses = []
    for o in orders:
        fs = o['financial_status']
        status_counts[fs] = status_counts.get(fs, 0) + 1
        if fs in ('refunded', 'partially_refunded'):
            refund_statuses.append(o['id'])

    print(f"\n=== Status Distribution ===")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status}: {count}")
    print(f"\nOrders with refund status: {len(refund_statuses)}")

    # Check which have Zip products
    zip_orders = [o for o in orders if any(
        li.get('vendor') == 'Zip' for li in o.get('line_items', [])
    )]
    zip_refunded = [o for o in zip_orders if o['financial_status'] in ('refunded', 'partially_refunded')]
    print(f"\nZip orders in update set: {len(zip_orders)}")
    print(f"Zip orders with refund status: {len(zip_refunded)}")

    for o in zip_refunded:
        diff = float(o['total_price']) - float(o['current_total_price'])
        print(f"  Order {o['id']}: total=${o['total_price']:,.2f}, current=${o['current_total_price']:,.2f}, refund_amount=${diff:,.2f}, status={o['financial_status']}")

    if args.dry_run:
        print("\n[DRY RUN] Would save orders and fetch refunds. Exiting.")
        return

    # Step 2: Save/update all orders (updates financial_status, current_total_price)
    print(f"\n=== Saving {len(orders)} orders ===")
    batch_size = 500
    total_created = 0
    total_updated = 0
    total_failed = 0

    for i in range(0, len(orders), batch_size):
        batch = orders[i:i + batch_size]
        data = {
            'orders': {
                'items': batch,
                'total_orders': len(batch),
            }
        }
        result = service._save_shopify_orders(data)
        total_created += result['created']
        total_updated += result['updated']
        total_failed += result['failed']
        print(f"  Batch {i // batch_size + 1}: {result['created']} created, {result['updated']} updated, {result['failed']} failed")

    print(f"Total: {total_created} created, {total_updated} updated, {total_failed} failed")

    # Step 3: Fetch refunds for ALL orders with refund status
    if refund_statuses:
        print(f"\n=== Fetching refunds for {len(refund_statuses)} orders ===")

        batch_size = 50
        total_refunds_saved = 0
        num_batches = (len(refund_statuses) + batch_size - 1) // batch_size

        for i in range(0, len(refund_statuses), batch_size):
            batch = refund_statuses[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"\n  Batch {batch_num}/{num_batches}: {len(batch)} orders")

            refund_items = await connector._fetch_refunds(batch)

            if refund_items:
                data = {'refunds': {'items': refund_items}}
                result = service._save_shopify_refunds(data)
                saved = result['created'] + result['updated']
                total_refunds_saved += saved
                print(f"  Saved {saved} refunds ({result['created']} new, {result['updated']} updated)")
            else:
                print(f"  No refunds found")

            del refund_items
            gc.collect()
            await asyncio.sleep(0.5)

        print(f"\nTotal refunds saved: {total_refunds_saved}")

    # Step 4: Verify
    print(f"\n=== Verification ===")
    db = SessionLocal()
    try:
        # All refund line items in Jan 2026
        r = db.execute(text('''
            SELECT COUNT(*), SUM(rli.subtotal)
            FROM shopify_refund_line_items rli
            JOIN shopify_refunds rf ON rf.shopify_refund_id = rli.shopify_refund_id
            WHERE rf.created_at >= '2026-01-01' AND rf.created_at < '2026-02-01'
        ''')).fetchone()
        print(f"All refund line items in Jan 2026: {r[0]}, total ${float(r[1] or 0):,.2f}")

        # Zip refund line items in Jan 2026
        r = db.execute(text('''
            SELECT COUNT(*), SUM(rli.subtotal)
            FROM shopify_refund_line_items rli
            JOIN shopify_refunds rf ON rf.shopify_refund_id = rli.shopify_refund_id
            JOIN shopify_order_items oi ON oi.line_item_id = rli.line_item_id
            WHERE oi.vendor = 'Zip'
              AND rf.created_at >= '2026-01-01' AND rf.created_at < '2026-02-01'
        ''')).fetchone()
        print(f"Zip refund line items in Jan 2026: {r[1]} items, ${float(r[0] or 0):,.2f}")
        print(f"Shopify reports Zip returns Jan 2026: $19,868.19")

        # Also check using current_total_price approach
        r = db.execute(text('''
            SELECT SUM(oi.price * oi.quantity) as gross,
                   SUM(CASE WHEN o.financial_status IN ('refunded', 'partially_refunded', 'voided')
                       THEN oi.price * oi.quantity ELSE 0 END) as refund_risk
            FROM shopify_order_items oi
            JOIN shopify_orders o ON o.shopify_order_id = oi.shopify_order_id
            WHERE oi.vendor = 'Zip'
              AND o.created_at >= '2026-01-01' AND o.created_at < '2026-02-01'
        ''')).fetchone()
        print(f"\nZip Jan 2026 orders: gross=${float(r[0] or 0):,.2f}, refund-status items=${float(r[1] or 0):,.2f}")

        # Check order financial_status distribution for Zip Jan 2026
        r = db.execute(text('''
            SELECT o.financial_status, COUNT(DISTINCT o.shopify_order_id), SUM(oi.price * oi.quantity)
            FROM shopify_order_items oi
            JOIN shopify_orders o ON o.shopify_order_id = oi.shopify_order_id
            WHERE oi.vendor = 'Zip'
              AND o.created_at >= '2026-01-01' AND o.created_at < '2026-02-01'
            GROUP BY o.financial_status
        ''')).fetchall()
        print(f"\nZip Jan 2026 by financial status:")
        for row in r:
            print(f"  {row[0]}: {row[1]} orders, ${float(row[2] or 0):,.2f}")

    finally:
        db.close()

    print(f"\n{'=' * 60}")
    print("Done!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
