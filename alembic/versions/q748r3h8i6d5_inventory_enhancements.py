"""Inventory enhancements: oversold/cost_missing/offline columns + daily snapshots

Revision ID: q748r3h8i6d5
Revises: p637q2g7h5c4
Create Date: 2026-01-31

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = 'q748r3h8i6d5'
down_revision: Union[str, None] = 'p637q2g7h5c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table_name, column_name):
    """Check if a column already exists in the table."""
    bind = op.get_bind()
    insp = inspect(bind)
    columns = [c['name'] for c in insp.get_columns(table_name)]
    return column_name in columns


def _has_table(table_name):
    """Check if a table exists."""
    bind = op.get_bind()
    insp = inspect(bind)
    return table_name in insp.get_table_names()


def upgrade() -> None:
    # Add new columns to ml_inventory_suggestions (if not already present)
    if not _has_column('ml_inventory_suggestions', 'oversold'):
        op.add_column('ml_inventory_suggestions', sa.Column('oversold', sa.Boolean(), nullable=False, server_default='false'))
    if not _has_column('ml_inventory_suggestions', 'cost_missing'):
        op.add_column('ml_inventory_suggestions', sa.Column('cost_missing', sa.Boolean(), nullable=False, server_default='false'))
    if not _has_column('ml_inventory_suggestions', 'offline_units_30d'):
        op.add_column('ml_inventory_suggestions', sa.Column('offline_units_30d', sa.Float(), nullable=False, server_default='0'))

    # Create inventory_daily_snapshots table (if not already present)
    if not _has_table('inventory_daily_snapshots'):
        op.create_table(
            'inventory_daily_snapshots',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('sku', sa.String(), nullable=False),
            sa.Column('snapshot_date', sa.Date(), nullable=False),
            sa.Column('quantity', sa.Integer(), nullable=False),
            sa.Column('synced_at', sa.DateTime(), nullable=True),
            sa.UniqueConstraint('sku', 'snapshot_date', name='uq_inv_snapshot_sku_date'),
        )
        op.create_index('ix_inv_snap_sku', 'inventory_daily_snapshots', ['sku'])
        op.create_index('ix_inv_snap_date', 'inventory_daily_snapshots', ['snapshot_date'])


def downgrade() -> None:
    if _has_table('inventory_daily_snapshots'):
        op.drop_table('inventory_daily_snapshots')
    if _has_column('ml_inventory_suggestions', 'offline_units_30d'):
        op.drop_column('ml_inventory_suggestions', 'offline_units_30d')
    if _has_column('ml_inventory_suggestions', 'cost_missing'):
        op.drop_column('ml_inventory_suggestions', 'cost_missing')
    if _has_column('ml_inventory_suggestions', 'oversold'):
        op.drop_column('ml_inventory_suggestions', 'oversold')
