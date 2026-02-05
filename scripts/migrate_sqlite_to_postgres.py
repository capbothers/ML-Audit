#!/usr/bin/env python3
"""
Migrate all data from SQLite (ml_audit.db) to Postgres (Neon).

Safe to re-run: truncates each target table before inserting.
Copies data in batches to stay within memory limits.

Usage:
    # Dry-run (show tables and row counts, no writes)
    python scripts/migrate_sqlite_to_postgres.py --dry-run

    # Full migration
    DATABASE_URL=postgresql://... python scripts/migrate_sqlite_to_postgres.py

    # Migrate specific tables only
    python scripts/migrate_sqlite_to_postgres.py --tables shopify_orders shopify_order_items
"""
import argparse
import os
import sys
import time

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import NullPool

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "ml_audit.db")
BATCH_SIZE = 2000

# Tables to skip (not real data, or Alembic-managed)
SKIP_TABLES = {"alembic_version", "__caprice_import_test"}


def get_src_engine():
    sqlite_url = f"sqlite:///{os.path.abspath(SQLITE_PATH)}"
    return create_engine(
        sqlite_url,
        connect_args={"check_same_thread": False, "timeout": 60},
        poolclass=NullPool,
    )


def get_dst_engine(pg_url: str):
    return create_engine(pg_url, pool_pre_ping=True, pool_size=5, max_overflow=10)


def get_table_counts(engine, tables: list[str]) -> dict[str, int]:
    counts = {}
    with engine.connect() as conn:
        for t in tables:
            try:
                row = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar()
                counts[t] = row or 0
            except Exception:
                counts[t] = -1
    return counts


def migrate_table(src_engine, dst_engine, table: str) -> int:
    """Truncate destination table, then batch-copy all rows. Returns rows copied."""
    with src_engine.connect() as src_conn:
        total = src_conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
        if total == 0:
            return 0

        # Get column names from source
        cols_result = src_conn.execute(text(f'PRAGMA table_info("{table}")'))
        columns = [row[1] for row in cols_result]

    # Truncate destination
    with dst_engine.connect() as dst_conn:
        # Check table exists in Postgres
        pg_tables = inspect(dst_engine).get_table_names()
        if table not in pg_tables:
            print(f"  SKIP {table}: not in Postgres schema")
            return -1

        # Get Postgres columns to find the intersection
        pg_col_info = {c["name"]: c for c in inspect(dst_engine).get_columns(table)}
        pg_cols = list(pg_col_info.keys())
        common_cols = [c for c in columns if c in pg_cols]
        if not common_cols:
            print(f"  SKIP {table}: no common columns")
            return -1

        # Identify boolean columns (SQLite stores as 0/1, Postgres needs True/False)
        bool_cols = {
            c for c in common_cols
            if str(pg_col_info[c]["type"]).upper() == "BOOLEAN"
        }

        dst_conn.execute(text(f'TRUNCATE TABLE "{table}" CASCADE'))
        dst_conn.commit()

    # Batch copy
    col_list = ", ".join(f'"{c}"' for c in common_cols)
    param_list = ", ".join(f":{c}" for c in common_cols)
    insert_sql = text(f'INSERT INTO "{table}" ({col_list}) VALUES ({param_list})')

    copied = 0
    with src_engine.connect() as src_conn:
        result = src_conn.execute(text(f'SELECT {col_list} FROM "{table}"'))

        batch = []
        for row in result:
            row_dict = dict(zip(common_cols, row))
            # Cast SQLite integer booleans to Python bools
            for col in bool_cols:
                v = row_dict[col]
                if v is not None:
                    row_dict[col] = bool(v)
            batch.append(row_dict)
            if len(batch) >= BATCH_SIZE:
                with dst_engine.connect() as dst_conn:
                    dst_conn.execute(insert_sql, batch)
                    dst_conn.commit()
                copied += len(batch)
                batch = []
                # Progress for large tables
                if total > 10000:
                    pct = (copied / total) * 100
                    print(f"    {copied:,}/{total:,} ({pct:.0f}%)", end="\r")

        if batch:
            with dst_engine.connect() as dst_conn:
                dst_conn.execute(insert_sql, batch)
                dst_conn.commit()
            copied += len(batch)

    if total > 10000:
        print()  # clear progress line

    return copied


def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite → Postgres")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only")
    parser.add_argument("--tables", nargs="+", help="Migrate specific tables only")
    args = parser.parse_args()

    pg_url = os.environ.get("DATABASE_URL", "")
    if not pg_url and not args.dry_run:
        print("ERROR: DATABASE_URL env var not set")
        sys.exit(1)

    if not os.path.exists(SQLITE_PATH):
        print(f"ERROR: SQLite file not found at {SQLITE_PATH}")
        sys.exit(1)

    # Connect
    src = get_src_engine()
    dst = get_dst_engine(pg_url) if pg_url else None

    # Get source tables
    with src.connect() as conn:
        rows = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        )
        all_tables = [r[0] for r in rows if r[0] not in SKIP_TABLES]

    tables = args.tables if args.tables else all_tables
    tables = [t for t in tables if t not in SKIP_TABLES]

    # Source counts
    src_counts = get_table_counts(src, tables)
    non_empty = {t: c for t, c in src_counts.items() if c > 0}

    print(f"\nSQLite: {len(all_tables)} tables, {len(non_empty)} with data\n")
    print(f"{'Table':<45} {'Rows':>12}")
    print("-" * 60)
    for t in sorted(non_empty, key=lambda x: non_empty[x], reverse=True):
        print(f"{t:<45} {non_empty[t]:>12,}")

    total_rows = sum(non_empty.values())
    print(f"\n{'TOTAL':<45} {total_rows:>12,}")

    if args.dry_run:
        print("\n[DRY RUN] No data written.")
        return

    # Migrate
    print(f"\n{'='*60}")
    print("Starting migration...")
    print(f"{'='*60}\n")

    start = time.time()
    results = {}

    for table in sorted(non_empty, key=lambda x: non_empty[x]):
        count = non_empty[table]
        print(f"  {table} ({count:,} rows)...", end=" ", flush=True)
        t0 = time.time()
        copied = migrate_table(src, dst, table)
        elapsed = time.time() - t0
        if copied >= 0:
            print(f"OK ({copied:,} copied, {elapsed:.1f}s)")
            results[table] = copied
        else:
            results[table] = 0

    total_time = time.time() - start

    # Validate
    print(f"\n{'='*60}")
    print("Validation: comparing row counts")
    print(f"{'='*60}\n")

    dst_counts = get_table_counts(dst, list(results.keys()))

    ok = True
    print(f"{'Table':<45} {'SQLite':>10} {'Postgres':>10} {'Match':>7}")
    print("-" * 75)
    for t in sorted(results):
        s = non_empty.get(t, 0)
        p = dst_counts.get(t, 0)
        match = "OK" if s == p else "MISMATCH"
        if match != "OK":
            ok = False
        print(f"{t:<45} {s:>10,} {p:>10,} {match:>7}")

    total_copied = sum(dst_counts.get(t, 0) for t in results)
    print(f"\n{'TOTAL':<45} {total_rows:>10,} {total_copied:>10,}")
    print(f"\nCompleted in {total_time:.0f}s")

    if ok:
        print("\nAll counts match. Migration successful.")
    else:
        print("\nWARNING: Some counts don't match. Check tables above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
