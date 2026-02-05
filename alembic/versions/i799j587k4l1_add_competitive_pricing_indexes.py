"""add_competitive_pricing_indexes

Revision ID: i799j587k4l1
Revises: h688i476j3k0
Create Date: 2026-01-28

Adds indexes to competitive_pricing table for query performance:
- pricing_date (for snapshot queries)
- variant_sku (for SKU lookups)
- vendor (for brand analysis)
- lowest_competitor_price (for undercut queries)
- minimum_price (for unmatchable queries)
- current_price (for gap calculations)
- Composite: (pricing_date, variant_sku) for SKU-specific queries
- Composite: (pricing_date, vendor) for brand queries
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'i799j587k4l1'
down_revision: Union[str, Sequence[str], None] = 'h688i476j3k0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add indexes to competitive_pricing table."""
    # Use raw SQL for SQLite to create indexes if not exists
    # Some basic indexes already exist from model definition, so we add composite and additional ones
    connection = op.get_bind()

    # Create indexes that don't exist (pricing_date, variant_sku, vendor may already exist)
    indexes_to_create = [
        ('ix_cp_lowest_competitor_price', 'lowest_competitor_price'),
        ('ix_cp_minimum_price', 'minimum_price'),
        ('ix_cp_current_price', 'current_price'),
    ]

    for idx_name, column in indexes_to_create:
        try:
            connection.execute(sa.text(
                f'CREATE INDEX IF NOT EXISTS {idx_name} ON competitive_pricing ({column})'
            ))
        except Exception:
            pass  # Index may already exist

    # Create composite indexes for common query patterns
    composite_indexes = [
        ('ix_cp_date_sku', 'pricing_date, variant_sku'),
        ('ix_cp_date_vendor', 'pricing_date, vendor'),
    ]

    for idx_name, columns in composite_indexes:
        try:
            connection.execute(sa.text(
                f'CREATE INDEX IF NOT EXISTS {idx_name} ON competitive_pricing ({columns})'
            ))
        except Exception:
            pass  # Index may already exist


def downgrade() -> None:
    """Remove indexes from competitive_pricing table."""
    connection = op.get_bind()

    indexes_to_drop = [
        'ix_cp_lowest_competitor_price',
        'ix_cp_minimum_price',
        'ix_cp_current_price',
        'ix_cp_date_sku',
        'ix_cp_date_vendor',
    ]

    for idx_name in indexes_to_drop:
        try:
            connection.execute(sa.text(f'DROP INDEX IF EXISTS {idx_name}'))
        except Exception:
            pass  # Index may not exist
