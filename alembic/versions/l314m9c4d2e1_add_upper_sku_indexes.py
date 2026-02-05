"""Add upper SKU indexes for faster joins

Revision ID: l314m9c4d2e1
Revises: k213l8b2c0d4
Create Date: 2026-01-28

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "l314m9c4d2e1"
down_revision: Union[str, Sequence[str], None] = "k213l8b2c0d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Expression indexes for case-insensitive SKU joins
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_competitive_pricing_variant_sku_upper "
        "ON competitive_pricing(upper(variant_sku))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_product_costs_vendor_sku_upper "
        "ON product_costs(upper(vendor_sku))"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP INDEX IF EXISTS ix_competitive_pricing_variant_sku_upper")
    op.execute("DROP INDEX IF EXISTS ix_product_costs_vendor_sku_upper")
