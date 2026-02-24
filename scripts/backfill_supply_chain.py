#!/usr/bin/env python3
"""
Backfill supply-chain defaults on ProductCost rows that have no lead_time_days.

Sets sensible defaults so the ML pipeline's confidence isn't dragged down
by "default_lead_time" data-issue flags across the entire catalogue.

Defaults (can be overridden via CLI):
  lead_time_days  = 14
  service_level   = 0.95
  moq             = 1
  case_pack       = 1

These are conservative placeholders.  Replace per-vendor once real data
is available in the Google Sheets pricing tab.

Usage:
    python scripts/backfill_supply_chain.py
    python scripts/backfill_supply_chain.py --lead-time 21 --service-level 0.90
    python scripts/backfill_supply_chain.py --vendor "BrandX" --lead-time 7
    python scripts/backfill_supply_chain.py --dry-run
"""
import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func
from app.models.base import SessionLocal, init_db
from app.models.product_cost import ProductCost

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def main():
    parser = argparse.ArgumentParser(
        description="Backfill supply-chain defaults on ProductCost rows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/backfill_supply_chain.py                        # all SKUs, default values
  python scripts/backfill_supply_chain.py --vendor "BrandX"      # one vendor only
  python scripts/backfill_supply_chain.py --lead-time 21         # custom lead time
  python scripts/backfill_supply_chain.py --dry-run              # preview only
""",
    )
    parser.add_argument("--lead-time", type=int, default=14, help="Default lead_time_days (default: 14)")
    parser.add_argument("--service-level", type=float, default=0.95, help="Default service_level (default: 0.95)")
    parser.add_argument("--moq", type=int, default=1, help="Default MOQ (default: 1)")
    parser.add_argument("--case-pack", type=int, default=1, help="Default case_pack (default: 1)")
    parser.add_argument("--vendor", type=str, default=None, help="Only backfill a specific vendor/brand")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing non-NULL values")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    # Ensure new columns exist in the DB
    init_db()

    db = SessionLocal()
    try:
        # Count rows needing backfill
        base_q = db.query(ProductCost).filter(ProductCost.vendor_sku.isnot(None))

        if args.vendor:
            base_q = base_q.filter(func.upper(ProductCost.vendor) == args.vendor.upper())

        if not args.overwrite:
            # Only rows missing at least one field
            base_q = base_q.filter(
                (ProductCost.lead_time_days.is_(None))
                | (ProductCost.service_level.is_(None))
                | (ProductCost.moq.is_(None))
                | (ProductCost.case_pack.is_(None))
            )

        total = base_q.count()
        total_all = db.query(func.count(ProductCost.id)).scalar() or 0

        logger.info("=" * 60)
        logger.info("Supply-Chain Backfill")
        logger.info("=" * 60)
        logger.info(f"Total ProductCost rows: {total_all}")
        logger.info(f"Rows to update: {total}")
        if args.vendor:
            logger.info(f"Vendor filter: {args.vendor}")
        logger.info(f"Values: lead_time={args.lead_time}d, service_level={args.service_level}, moq={args.moq}, case_pack={args.case_pack}")
        if args.overwrite:
            logger.info("Mode: OVERWRITE existing values")
        if args.dry_run:
            logger.info("Mode: DRY RUN (no changes will be saved)")
        logger.info("=" * 60)

        if total == 0:
            logger.info("Nothing to do — all rows already have supply-chain data.")
            return

        if args.dry_run:
            # Show sample of SKUs that would be updated
            sample = base_q.limit(10).all()
            logger.info(f"Sample SKUs that would be updated (first 10):")
            for row in sample:
                logger.info(f"  {row.vendor_sku} | vendor={row.vendor} | lt={row.lead_time_days} sl={row.service_level} moq={row.moq} cp={row.case_pack}")
            logger.info(f"... and {max(0, total - 10)} more")
            return

        # Process in batches
        updated = 0
        offset = 0
        while offset < total:
            batch = base_q.offset(offset).limit(BATCH_SIZE).all()
            if not batch:
                break

            for row in batch:
                if args.overwrite or row.lead_time_days is None:
                    row.lead_time_days = args.lead_time
                if args.overwrite or row.service_level is None:
                    row.service_level = args.service_level
                if args.overwrite or row.moq is None:
                    row.moq = args.moq
                if args.overwrite or row.case_pack is None:
                    row.case_pack = args.case_pack
                updated += 1

            db.commit()
            offset += BATCH_SIZE
            logger.info(f"  Processed {min(offset, total)}/{total}")

        logger.info("=" * 60)
        logger.info(f"Backfill complete: {updated} rows updated")

        # Verify
        remaining = (
            db.query(func.count(ProductCost.id))
            .filter(ProductCost.lead_time_days.is_(None))
            .scalar()
        ) or 0
        logger.info(f"Rows still missing lead_time_days: {remaining}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
