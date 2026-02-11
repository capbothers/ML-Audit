"""
Backfill Shippit shipping costs for historical orders.
Runs directly (not through the API) to avoid HTTP timeouts.

Usage: python scripts/backfill_shippit.py [--days 60]
"""
import asyncio
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from decimal import Decimal
import pytz

from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrder
from app.models.shippit import ShippitOrder
from app.connectors.shopify_connector import ShopifyConnector
from app.connectors.shippit_connector import ShippitConnector
from app.config import get_settings
from sqlalchemy import or_

SYDNEY = pytz.timezone("Australia/Sydney")
settings = get_settings()


async def backfill(days: int, batch_size: int = 50):
    now = datetime.now(SYDNEY)
    start = now - timedelta(days=days)

    db = SessionLocal()

    # Find fulfilled orders missing Shippit data
    fulfilled = (
        db.query(ShopifyOrder.shopify_order_id, ShopifyOrder.order_number)
        .outerjoin(ShippitOrder, ShippitOrder.shopify_order_id == ShopifyOrder.shopify_order_id)
        .filter(
            ShopifyOrder.created_at >= start,
            ShopifyOrder.fulfillment_status.in_(["fulfilled", "partial"]),
            ShippitOrder.id.is_(None),
        )
        .all()
    )
    db.close()

    print(f"Found {len(fulfilled)} fulfilled orders missing Shippit data ({days} days)")
    if not fulfilled:
        return

    # Init connectors
    shopify = ShopifyConnector()
    await shopify.connect()
    shippit = ShippitConnector()

    # Process in batches
    total_saved = 0
    total_quoted = 0
    for i in range(0, len(fulfilled), batch_size):
        batch = fulfilled[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(fulfilled) + batch_size - 1) // batch_size
        order_ids = [o.shopify_order_id for o in batch]

        print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} orders) ---")

        # Step 1: Get tracking numbers from Shopify
        tracking_map = await shopify.fetch_fulfillment_tracking(order_ids)
        all_tracking = []
        # Map tracking -> shopify_order_id for later
        tn_to_order_id = {}
        for oid, tns in tracking_map.items():
            for tn in tns:
                all_tracking.append(tn)
                tn_to_order_id[tn] = oid

        if not all_tracking:
            print(f"  No tracking numbers found, skipping")
            continue

        print(f"  Found {len(all_tracking)} tracking numbers")

        # Step 2: Look up in Shippit + get cost estimates
        orders = await shippit._fetch_by_tracking_numbers(all_tracking)
        print(f"  Fetched {len(orders)} orders from Shippit")

        # Step 3: Get cost estimates via Quote API
        import aiohttp
        quoted = 0
        async with aiohttp.ClientSession() as session:
            for order in orders:
                cost = await shippit._estimate_shipping_cost(session, order)
                if cost is not None:
                    order["shipping_cost"] = cost
                    quoted += 1
        print(f"  Got {quoted} cost estimates")
        total_quoted += quoted

        # Step 4: Save to database
        db = SessionLocal()
        saved = 0
        for order_data in orders:
            tracking = order_data.get("tracking_number")
            if not tracking:
                continue

            existing = db.query(ShippitOrder).filter(ShippitOrder.tracking_number == tracking).first()
            if existing:
                continue

            # Resolve shopify_order_id
            ref_str = order_data.get("shopify_order_id_from_ref", "")
            shopify_order_id = None
            if ref_str:
                try:
                    shopify_order_id = int(ref_str)
                except (ValueError, TypeError):
                    pass
            if not shopify_order_id:
                inv = order_data.get("retailer_order_number", "")
                if inv.startswith("INT"):
                    try:
                        order_num = int(inv.replace("INT", ""))
                        match = db.query(ShopifyOrder.shopify_order_id).filter(
                            ShopifyOrder.order_number == order_num
                        ).first()
                        if match:
                            shopify_order_id = match[0]
                    except (ValueError, TypeError):
                        pass

            cost = order_data.get("shipping_cost")
            shipping_cost = Decimal(str(cost)) if cost is not None else None

            new_order = ShippitOrder(
                tracking_number=tracking,
                retailer_order_number=order_data.get("retailer_order_number"),
                shopify_order_id=shopify_order_id,
                courier_name=order_data.get("courier_name"),
                courier_type=order_data.get("courier_type"),
                service_level=order_data.get("service_level"),
                shipping_cost=shipping_cost,
                state=order_data.get("state"),
                parcel_count=order_data.get("parcel_count", 1),
                raw_response=order_data.get("raw_response"),
                synced_at=datetime.utcnow(),
            )
            db.add(new_order)
            saved += 1

        db.commit()
        db.close()
        total_saved += saved
        print(f"  Saved {saved} new records")

    print(f"\n=== DONE ===")
    print(f"Total saved: {total_saved}")
    print(f"Total with cost estimates: {total_quoted}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--batch", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(backfill(args.days, args.batch))
