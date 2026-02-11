"""
Backfill cost_per_item on ShopifyOrderItem using ProductCost.vendor_sku lookup.

Supports three matching strategies:
1. Exact match (vendor_sku == order SKU)
2. Description-prefix match (first token of description == order SKU)
   - Needed for Oliveri: cost table uses numeric IDs but description starts with model code
3. Base-model match (strip generation suffix like G7, G5/P from order SKU)
   - Needed for Rheem: Shopify SKUs have generation suffixes not in cost table

Usage:
    PYTHONPATH=/workspaces/ML-Audit python3 scripts/backfill_order_item_costs.py
"""
import re
import sys
import logging

sys.path.insert(0, "/workspaces/ML-Audit")

from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrderItem
from app.models.product_cost import ProductCost

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def build_cost_lookup(db):
    """
    Build comprehensive SKU -> cost lookup with multiple matching strategies.

    Returns dict mapping SKU (string) -> cost (Decimal).
    """
    costs = db.query(
        ProductCost.vendor_sku,
        ProductCost.description,
        ProductCost.nett_nett_cost_inc_gst,
        ProductCost.has_active_special,
        ProductCost.special_cost_inc_gst,
    ).filter(
        ProductCost.nett_nett_cost_inc_gst.isnot(None),
    ).all()

    cost_map = {}          # exact SKU match
    cost_map_lower = {}    # case-insensitive SKU match
    desc_prefix_map = {}   # first token of description -> cost (for Oliveri)

    for sku, description, nett_cost, has_special, special_cost in costs:
        if not sku or not nett_cost:
            continue

        # Use active cost (special if active, otherwise nett nett)
        active_cost = special_cost if has_special and special_cost else nett_cost

        # Strategy 1: exact and case-insensitive
        cost_map[sku] = active_cost
        cost_map_lower[sku.lower()] = active_cost

        # Strategy 2: description prefix (first token before space)
        # e.g. "FR5910 Replacement Cartridge..." -> key "FR5910"
        if description:
            first_token = description.split()[0] if description.strip() else None
            if first_token and first_token != sku:
                # Only add if the token looks like a model code (has letters)
                if re.search(r'[A-Za-z]', first_token):
                    desc_prefix_map[first_token] = active_cost
                    desc_prefix_map[first_token.upper()] = active_cost

    logger.info(f"  Exact SKU entries: {len(cost_map)}")
    logger.info(f"  Description-prefix entries: {len(desc_prefix_map)}")

    return cost_map, cost_map_lower, desc_prefix_map


def lookup_cost(sku, cost_map, cost_map_lower, desc_prefix_map):
    """
    Try multiple strategies to find cost for a SKU.

    Returns (cost, match_type) or (None, "no_match").
    """
    # Strategy 1: Exact match
    if sku in cost_map:
        return cost_map[sku], "exact"

    # Strategy 2: Case-insensitive
    if sku.lower() in cost_map_lower:
        return cost_map_lower[sku.lower()], "case_insensitive"

    # Strategy 3: Description-prefix match (for Oliveri-style numeric vendor_sku)
    if sku in desc_prefix_map:
        return desc_prefix_map[sku], "desc_prefix"
    if sku.upper() in desc_prefix_map:
        return desc_prefix_map[sku.upper()], "desc_prefix"

    # Strategy 4: Strip generation suffix for Rheem-style SKUs
    # e.g. 191050G7 -> 191050, 491125G7-INSTALL -> 491125, 551E280G5 -> 551E280
    base_sku = re.sub(r'G\d.*$', '', sku)
    if base_sku != sku:
        if base_sku in cost_map:
            return cost_map[base_sku], "base_model"
        if base_sku.lower() in cost_map_lower:
            return cost_map_lower[base_sku.lower()], "base_model"

    return None, "no_match"


def main():
    db = SessionLocal()

    try:
        logger.info("Building SKU -> cost lookup from product_costs...")
        cost_map, cost_map_lower, desc_prefix_map = build_cost_lookup(db)

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
            "exact": 0,
            "case_insensitive": 0,
            "desc_prefix": 0,
            "base_model": 0,
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
                cost, match_type = lookup_cost(sku, cost_map, cost_map_lower, desc_prefix_map)

                if cost is not None:
                    item.cost_per_item = cost

                stats[match_type] += 1
                stats["processed"] += 1

            db.commit()
            offset += BATCH_SIZE
            logger.info(f"  Processed {min(offset, total)}/{total}")

        matched = stats["processed"] - stats["no_match"]
        logger.info("=" * 60)
        logger.info(f"COGS backfill complete:")
        logger.info(f"  Total processed:         {stats['processed']}")
        logger.info(f"  Exact match:             {stats['exact']}")
        logger.info(f"  Case-insensitive:        {stats['case_insensitive']}")
        logger.info(f"  Description-prefix:      {stats['desc_prefix']} (Oliveri-style)")
        logger.info(f"  Base-model match:        {stats['base_model']} (Rheem-style)")
        logger.info(f"  Total matched:           {matched} ({matched/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  No match:                {stats['no_match']} ({stats['no_match']/max(stats['processed'],1)*100:.1f}%)")

    finally:
        db.close()


if __name__ == "__main__":
    main()
