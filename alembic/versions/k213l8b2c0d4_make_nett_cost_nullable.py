"""Make nett_nett_cost_inc_gst nullable

Revision ID: k213l8b2c0d4
Revises: j102k7a1b9c3
Create Date: 2026-01-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'k213l8b2c0d4'
down_revision: Union[str, Sequence[str], None] = 'j102k7a1b9c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Make nett_nett_cost_inc_gst nullable to allow partial data sync."""
    # SQLite doesn't support ALTER COLUMN, so we need to recreate the table
    # For simplicity, we'll just drop and recreate the constraint
    # In SQLite, this can be done with a batch operation
    with op.batch_alter_table('product_costs') as batch_op:
        batch_op.alter_column(
            'nett_nett_cost_inc_gst',
            existing_type=sa.Numeric(10, 2),
            nullable=True
        )


def downgrade() -> None:
    """Make nett_nett_cost_inc_gst NOT NULL again."""
    with op.batch_alter_table('product_costs') as batch_op:
        batch_op.alter_column(
            'nett_nett_cost_inc_gst',
            existing_type=sa.Numeric(10, 2),
            nullable=False
        )
