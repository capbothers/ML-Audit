"""
Backfill cost_per_item on ShopifyOrderItem using ProductCost.vendor_sku lookup.

One-time script to populate COGS data for profitability analysis.

Usage:
    PYTHONPATH=/workspaces/ML-Audit python3 scripts/backfill_order_item_costs.py
"""
import sys
import logging

sys.path.insert(0, "/workspaces/ML-Audit")

from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrderItem
from app.models.product_cost import ProductCost

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def main():
    db = SessionLocal()

    try:
        # Build SKU → cost lookup from ProductCost
        logger.info("Building SKU → cost lookup from product_costs...")
        costs = db.query(
            ProductCost.vendor_sku,
            ProductCost.nett_nett_cost_inc_gst,
        ).filter(
            ProductCost.nett_nett_cost_inc_gst.isnot(None),
        ).all()

        cost_map = {}
        cost_map_lower = {}
        for sku, cost in costs:
            if sku and cost:
                cost_map[sku] = cost
                cost_map_lower[sku.lower()] = cost

        logger.info(f"  Loaded {len(cost_map)} SKUs with costs")

        # Count items needing backfill
        total = db.query(ShopifyOrderItem).filter(
            ShopifyOrderItem.cost_per_item.is_(None),
            ShopifyOrderItem.sku.isnot(None),
            ShopifyOrderItem.sku != "",
        ).count()

        logger.info(f"Found {total} order items needing cost backfill")

        offset = 0
        stats = {
            "processed": 0,
            "exact_match": 0,
            "case_insensitive_match": 0,
            "no_match": 0,
        }

        while offset < total:
            items = (
                db.query(ShopifyOrderItem)
                .filter(
                    ShopifyOrderItem.cost_per_item.is_(None),
                    ShopifyOrderItem.sku.isnot(None),
                    ShopifyOrderItem.sku != "",
                )
                .order_by(ShopifyOrderItem.id)
                .offset(offset)
                .limit(BATCH_SIZE)
                .all()
            )

            if not items:
                break

            for item in items:
                sku = item.sku.strip()

                # Exact match first
                if sku in cost_map:
                    item.cost_per_item = cost_map[sku]
                    stats["exact_match"] += 1
                # Case-insensitive fallback
                elif sku.lower() in cost_map_lower:
                    item.cost_per_item = cost_map_lower[sku.lower()]
                    stats["case_insensitive_match"] += 1
                else:
                    stats["no_match"] += 1

                stats["processed"] += 1

            db.commit()
            offset += BATCH_SIZE
            logger.info(f"  Processed {min(offset, total)}/{total}")

        matched = stats["exact_match"] + stats["case_insensitive_match"]
        logger.info("=" * 60)
        logger.info(f"COGS backfill complete:")
        logger.info(f"  Total processed:         {stats['processed']}")
        logger.info(f"  Exact match:             {stats['exact_match']} ({stats['exact_match']/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  Case-insensitive match:  {stats['case_insensitive_match']}")
        logger.info(f"  Total matched:           {matched} ({matched/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  No match:                {stats['no_match']} ({stats['no_match']/max(stats['processed'],1)*100:.1f}%)")

    finally:
        db.close()


if __name__ == "__main__":
    main()
