"""Add GA4 daily ecommerce table and event revenue column

Revision ID: e355f143e0c7
Revises: d244e032d9b6
Create Date: 2026-01-27 03:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e355f143e0c7'
down_revision: Union[str, Sequence[str], None] = 'd244e032d9b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create ga4_daily_ecommerce table for Shopify reconciliation
    op.create_table(
        'ga4_daily_ecommerce',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('ecommerce_purchases', sa.Integer(), default=0),
        sa.Column('total_revenue', sa.Numeric(10, 2), default=0),
        sa.Column('add_to_carts', sa.Integer(), default=0),
        sa.Column('checkouts', sa.Integer(), default=0),
        sa.Column('items_viewed', sa.Integer(), default=0),
        sa.Column('cart_to_purchase_rate', sa.Float(), nullable=True),
        sa.Column('synced_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_ga4_daily_ecommerce_id'), 'ga4_daily_ecommerce', ['id'], unique=False)
    op.create_index(op.f('ix_ga4_daily_ecommerce_date'), 'ga4_daily_ecommerce', ['date'], unique=True)

    # Add total_revenue column to ga4_events table
    op.add_column('ga4_events', sa.Column('total_revenue', sa.Numeric(10, 2), default=0))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove total_revenue column from ga4_events
    op.drop_column('ga4_events', 'total_revenue')

    # Drop ga4_daily_ecommerce table
    op.drop_index(op.f('ix_ga4_daily_ecommerce_date'), table_name='ga4_daily_ecommerce')
    op.drop_index(op.f('ix_ga4_daily_ecommerce_id'), table_name='ga4_daily_ecommerce')
    op.drop_table('ga4_daily_ecommerce')
