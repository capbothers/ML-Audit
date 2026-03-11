"""
One-off migration: clean up non-canonical source_name keys in data_sync_status.

Merges phantom rows (google_sheets_costs, product_costs, shopify_orders, etc.)
into their canonical equivalents, then deletes the duplicates.

Safe to run multiple times (idempotent).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.base import init_db, SessionLocal
from app.models.data_quality import DataSyncStatus
from app.freshness import normalize_key

init_db()
db = SessionLocal()

try:
    rows = db.query(DataSyncStatus).all()
    to_delete = []

    for row in rows:
        canonical = normalize_key(row.source_name)
        if canonical == row.source_name:
            continue  # already canonical, skip

        print(f"Non-canonical row: {row.source_name!r} → {canonical!r}  status={row.sync_status}")

        canonical_row = db.query(DataSyncStatus).filter_by(source_name=canonical).first()

        if canonical_row is None:
            # No canonical row exists — rename in place
            print(f"  Renaming {row.source_name!r} → {canonical!r}")
            row.source_name = canonical
        else:
            # Canonical row exists — keep the one with a more recent last_successful_sync
            row_ts = row.last_successful_sync
            can_ts = canonical_row.last_successful_sync
            if row_ts and (can_ts is None or row_ts > can_ts):
                print(f"  Non-canonical row is newer — merging into canonical")
                canonical_row.last_successful_sync = row_ts
                canonical_row.sync_status = row.sync_status
                canonical_row.last_sync_attempt = row.last_sync_attempt
                canonical_row.last_error = row.last_error
            else:
                print(f"  Canonical row is current — discarding non-canonical")
            to_delete.append(row)

    for row in to_delete:
        db.delete(row)

    db.commit()
    print(f"\nDone. {len(to_delete)} duplicate row(s) removed.")

except Exception as e:
    db.rollback()
    print(f"ERROR: {e}")
    raise
finally:
    db.close()
