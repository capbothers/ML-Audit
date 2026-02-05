"""add_shopify_order_items_table

Revision ID: 75d588541066
Revises: b92c4622f9e5
Create Date: 2026-01-26

Normalizes order line items for fast product analytics.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '75d588541066'
down_revision: Union[str, Sequence[str], None] = 'b92c4622f9e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create shopify_order_items table with indexes for fast product analytics."""
    op.create_table(
        'shopify_order_items',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),

        # Order reference
        sa.Column('shopify_order_id', sa.BigInteger(), nullable=False, index=True),
        sa.Column('order_number', sa.Integer(), index=True),

        # Denormalized order date for fast date-range queries
        sa.Column('order_date', sa.DateTime(), nullable=False, index=True),

        # Product identifiers
        sa.Column('shopify_product_id', sa.BigInteger(), index=True, nullable=True),
        sa.Column('shopify_variant_id', sa.BigInteger(), index=True, nullable=True),
        sa.Column('sku', sa.String(), index=True, nullable=True),

        # Product info
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('variant_title', sa.String(), nullable=True),
        sa.Column('vendor', sa.String(), index=True, nullable=True),
        sa.Column('product_type', sa.String(), index=True, nullable=True),

        # Quantities and amounts
        sa.Column('quantity', sa.Integer(), nullable=False, default=1),
        sa.Column('price', sa.Numeric(10, 2), nullable=False),
        sa.Column('total_price', sa.Numeric(10, 2), nullable=False),
        sa.Column('total_discount', sa.Numeric(10, 2), default=0),

        # For profitability
        sa.Column('cost_per_item', sa.Numeric(10, 2), nullable=True),

        # Order context (denormalized for fast filtering)
        sa.Column('financial_status', sa.String(), index=True, nullable=True),
        sa.Column('fulfillment_status', sa.String(), nullable=True),

        # Sync metadata
        sa.Column('synced_at', sa.DateTime()),
    )

    # Composite indexes for common query patterns
    op.create_index(
        'ix_order_items_date_product',
        'shopify_order_items',
        ['order_date', 'shopify_product_id']
    )
    op.create_index(
        'ix_order_items_date_sku',
        'shopify_order_items',
        ['order_date', 'sku']
    )
    op.create_index(
        'ix_order_items_product_date',
        'shopify_order_items',
        ['shopify_product_id', 'order_date']
    )

    # Foreign key (optional - can be disabled for performance)
    # op.create_foreign_key(
    #     'fk_order_items_order',
    #     'shopify_order_items',
    #     'shopify_orders',
    #     ['shopify_order_id'],
    #     ['shopify_order_id']
    # )


def downgrade() -> None:
    """Drop shopify_order_items table."""
    op.drop_index('ix_order_items_product_date', table_name='shopify_order_items')
    op.drop_index('ix_order_items_date_sku', table_name='shopify_order_items')
    op.drop_index('ix_order_items_date_product', table_name='shopify_order_items')
    op.drop_table('shopify_order_items')
