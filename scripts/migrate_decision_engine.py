"""
Migration script for Decision Engine capabilities.
Adds new columns to campaign_performance and creates new tables.
Safe to run multiple times (uses IF NOT EXISTS / try-except).
"""
import sys
sys.path.insert(0, '.')

from app.models.base import SessionLocal, engine, Base
import sqlalchemy as sa


def migrate():
    db = SessionLocal()
    try:
        # New columns on campaign_performance
        new_columns = [
            # Causal triage (Cap 1)
            ("primary_cause", "VARCHAR"),
            ("cause_confidence", "FLOAT"),
            ("cause_evidence", "JSON"),
            # Attribution confidence (Cap 2)
            ("attribution_confidence", "VARCHAR"),
            ("attribution_gap_pct", "FLOAT"),
            # Landing page friction (Cap 3)
            ("lp_cvr_change", "FLOAT"),
            ("lp_bounce_change", "FLOAT"),
            ("lp_is_friction", "BOOLEAN"),
        ]

        for col_name, col_type in new_columns:
            try:
                db.execute(sa.text(
                    f"ALTER TABLE campaign_performance ADD COLUMN {col_name} {col_type}"
                ))
                db.commit()
                print(f"  Added column: campaign_performance.{col_name}")
            except Exception:
                db.rollback()
                print(f"  Column already exists: campaign_performance.{col_name}")

        # Create new tables (decision_snapshots) via create_all
        # Import model so Base knows about it
        from app.models.decision_feedback import DecisionSnapshot  # noqa
        Base.metadata.create_all(bind=engine)
        print("  Created table: decision_snapshots (if not exists)")

        print("\nMigration complete.")
    finally:
        db.close()


if __name__ == "__main__":
    migrate()
