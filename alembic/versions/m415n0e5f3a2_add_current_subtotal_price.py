"""Add current_subtotal_price to shopify_orders

Revision ID: m415n0e5f3a2
Revises: l314m9c4d2e1
Create Date: 2026-01-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "m415n0e5f3a2"
down_revision: Union[str, Sequence[str], None] = "l314m9c4d2e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "shopify_orders",
        sa.Column("current_subtotal_price", sa.Numeric(10, 2), nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("shopify_orders", "current_subtotal_price")
