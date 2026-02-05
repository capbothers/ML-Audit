#!/usr/bin/env python3
"""
Backfill shopify_order_items table from existing orders.

Extracts line_items JSON from shopify_orders and normalizes into
the shopify_order_items table for fast product analytics.

Usage: python scripts/backfill_order_items.py
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime
from sqlalchemy import func
from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrder, ShopifyOrderItem

def backfill_order_items():
    """Backfill order items from existing orders."""
    db = SessionLocal()

    try:
        # Count existing order items
        existing_items = db.query(func.count(ShopifyOrderItem.id)).scalar() or 0
        print(f"Existing order items: {existing_items}")

        # Count orders
        order_count = db.query(func.count(ShopifyOrder.id)).scalar() or 0
        print(f"Total orders to process: {order_count}")

        if existing_items > 0:
            confirm = input(f"Found {existing_items} existing items. Delete and rebuild? (y/N): ")
            if confirm.lower() != 'y':
                print("Aborted.")
                return

            print("Deleting existing order items...")
            db.query(ShopifyOrderItem).delete()
            db.commit()

        # Process orders in batches
        batch_size = 500
        processed = 0
        items_created = 0

        orders = db.query(ShopifyOrder).filter(
            ShopifyOrder.line_items.isnot(None)
        ).yield_per(batch_size)

        print(f"\nProcessing orders...")

        for order in orders:
            line_items = order.line_items or []

            if not isinstance(line_items, list):
                continue

            for item in line_items:
                if not isinstance(item, dict):
                    continue

                order_item = ShopifyOrderItem(
                    shopify_order_id=order.shopify_order_id,
                    order_number=order.order_number,
                    order_date=order.created_at,

                    shopify_product_id=item.get("product_id"),
                    shopify_variant_id=item.get("variant_id"),
                    sku=item.get("sku"),

                    title=item.get("title"),
                    variant_title=item.get("variant_title"),
                    vendor=item.get("vendor"),
                    product_type=item.get("product_type"),

                    quantity=int(item.get("quantity", 1)),
                    price=float(item.get("price", 0)),
                    total_price=float(item.get("price", 0)) * int(item.get("quantity", 1)),
                    total_discount=float(item.get("total_discount", 0)) if item.get("total_discount") else 0,

                    financial_status=order.financial_status,
                    fulfillment_status=order.fulfillment_status,

                    synced_at=datetime.utcnow()
                )
                db.add(order_item)
                items_created += 1

            processed += 1
            if processed % 1000 == 0:
                db.commit()
                print(f"  Processed {processed} orders, {items_created} items created...")

        db.commit()
        print(f"\nDone! Processed {processed} orders, created {items_created} order items.")

        # Verify
        final_count = db.query(func.count(ShopifyOrderItem.id)).scalar() or 0
        print(f"Final order items count: {final_count}")

    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    backfill_order_items()
