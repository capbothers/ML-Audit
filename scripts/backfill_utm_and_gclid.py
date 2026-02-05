"""
Backfill UTM parameters, gclid, and gad_campaign_id from landing_site URLs.

One-time script to parse existing ShopifyOrder.landing_site values and
populate the utm_*, gclid, and gad_campaign_id columns.

Usage:
    PYTHONPATH=/workspaces/ML-Audit python3 scripts/backfill_utm_and_gclid.py
"""
import sys
import logging

sys.path.insert(0, "/workspaces/ML-Audit")

from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrder
from app.utils.url_parsing import parse_landing_site

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def main():
    db = SessionLocal()

    try:
        total = db.query(ShopifyOrder).filter(
            ShopifyOrder.landing_site.isnot(None),
            ShopifyOrder.landing_site != "",
        ).count()

        logger.info(f"Found {total} orders with landing_site to parse")

        offset = 0
        stats = {
            "processed": 0,
            "utm_campaign_found": 0,
            "gclid_found": 0,
            "gad_campaign_id_found": 0,
            "utm_source_found": 0,
            "nothing_found": 0,
        }

        while offset < total:
            orders = (
                db.query(ShopifyOrder)
                .filter(
                    ShopifyOrder.landing_site.isnot(None),
                    ShopifyOrder.landing_site != "",
                )
                .order_by(ShopifyOrder.id)
                .offset(offset)
                .limit(BATCH_SIZE)
                .all()
            )

            if not orders:
                break

            for order in orders:
                parsed = parse_landing_site(order.landing_site)
                found_something = False

                if parsed["utm_source"] and not order.utm_source:
                    order.utm_source = parsed["utm_source"]
                    stats["utm_source_found"] += 1
                    found_something = True

                if parsed["utm_medium"] and not order.utm_medium:
                    order.utm_medium = parsed["utm_medium"]
                    found_something = True

                if parsed["utm_campaign"] and not order.utm_campaign:
                    order.utm_campaign = parsed["utm_campaign"]
                    stats["utm_campaign_found"] += 1
                    found_something = True

                if parsed["utm_term"] and not order.utm_term:
                    order.utm_term = parsed["utm_term"]

                if parsed["utm_content"] and not order.utm_content:
                    order.utm_content = parsed["utm_content"]

                if parsed["gclid"] and not order.gclid:
                    order.gclid = parsed["gclid"]
                    stats["gclid_found"] += 1
                    found_something = True

                if parsed["gad_campaign_id"] and not order.gad_campaign_id:
                    order.gad_campaign_id = parsed["gad_campaign_id"]
                    stats["gad_campaign_id_found"] += 1
                    found_something = True

                if not found_something:
                    stats["nothing_found"] += 1

                stats["processed"] += 1

            db.commit()
            offset += BATCH_SIZE
            logger.info(f"  Processed {min(offset, total)}/{total}")

        logger.info("=" * 60)
        logger.info(f"Backfill complete:")
        logger.info(f"  Total processed:      {stats['processed']}")
        logger.info(f"  gclid found:          {stats['gclid_found']} ({stats['gclid_found']/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  gad_campaign_id found: {stats['gad_campaign_id_found']} ({stats['gad_campaign_id_found']/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  utm_campaign found:   {stats['utm_campaign_found']} ({stats['utm_campaign_found']/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  utm_source found:     {stats['utm_source_found']} ({stats['utm_source_found']/max(stats['processed'],1)*100:.1f}%)")
        logger.info(f"  nothing found:        {stats['nothing_found']} ({stats['nothing_found']/max(stats['processed'],1)*100:.1f}%)")

    finally:
        db.close()


if __name__ == "__main__":
    main()
