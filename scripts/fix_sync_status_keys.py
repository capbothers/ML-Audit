"""
One-off migration: clean up non-canonical source_name keys in data_sync_status.
Uses raw SQL to avoid ORM mapper initialisation overhead.
Safe to run multiple times (idempotent).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from app.config import get_settings

engine = create_engine(get_settings().database_url)

NON_CANONICAL = ('google_sheets_costs', 'product_costs', 'shopify_orders')

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT source_name, sync_status, last_successful_sync FROM data_sync_status WHERE source_name IN :names"),
        {"names": NON_CANONICAL},
    )
    rows = result.fetchall()

    if not rows:
        print("No non-canonical rows found — nothing to do.")
    else:
        for row in rows:
            print(f"Deleting: {row[0]}  status={row[1]}  last_sync={row[2]}")
        conn.execute(
            text("DELETE FROM data_sync_status WHERE source_name IN :names"),
            {"names": NON_CANONICAL},
        )
        conn.commit()
        print(f"Done. {len(rows)} row(s) removed.")
