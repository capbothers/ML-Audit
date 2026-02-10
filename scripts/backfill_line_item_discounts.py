"""
Backfill total_discount on shopify_order_items.

The ShopifyConnector._extract_line_items() never included total_discount,
so all existing order items have total_discount = 0. This script distributes
the order-level total_discounts proportionally across each order's line items
based on their share of total_price.

Only affects orders where shopify_orders.total_discounts > 0.
"""
import sqlite3
from decimal import Decimal, ROUND_HALF_UP

DB_PATH = "/workspaces/ML-Audit/ml_audit.db"


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Find orders with discounts
    orders = db.execute("""
        SELECT shopify_order_id, total_discounts
        FROM shopify_orders
        WHERE total_discounts > 0
    """).fetchall()

    print(f"Orders with discounts: {len(orders)}")

    updated = 0
    total_distributed = Decimal("0")

    for order in orders:
        oid = order["shopify_order_id"]
        order_discount = Decimal(str(order["total_discounts"]))

        # Get line items for this order
        items = db.execute("""
            SELECT id, total_price, quantity
            FROM shopify_order_items
            WHERE shopify_order_id = ?
        """, (oid,)).fetchall()

        if not items:
            continue

        # Calculate total order value from line items
        order_total = sum(Decimal(str(it["total_price"])) for it in items)
        if order_total <= 0:
            continue

        # Distribute discount proportionally
        remaining = order_discount
        for i, item in enumerate(items):
            item_total = Decimal(str(item["total_price"]))
            if i == len(items) - 1:
                # Last item gets remainder to avoid rounding errors
                item_discount = remaining
            else:
                share = item_total / order_total
                item_discount = (order_discount * share).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                remaining -= item_discount

            if item_discount > 0:
                db.execute(
                    "UPDATE shopify_order_items SET total_discount = ? WHERE id = ?",
                    (float(item_discount), item["id"])
                )
                updated += 1
                total_distributed += item_discount

    db.commit()
    db.close()

    print(f"Updated {updated} line items across {len(orders)} orders")
    print(f"Total discount distributed: ${total_distributed:,.2f}")


if __name__ == "__main__":
    main()
