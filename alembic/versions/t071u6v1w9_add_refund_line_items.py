"""Add line_item_id to order items and refund line item table

Revision ID: t071u6v1w9
Revises: s960t5j0k8f7
Create Date: 2026-02-09
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = 't071u6v1w9'
down_revision: Union[str, None] = 's960t5j0k8f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(table_name):
    bind = op.get_bind()
    insp = inspect(bind)
    return table_name in insp.get_table_names()


def _has_column(table_name, column_name):
    bind = op.get_bind()
    insp = inspect(bind)
    columns = [c['name'] for c in insp.get_columns(table_name)]
    return column_name in columns


def upgrade() -> None:
    if _has_table('shopify_order_items') and not _has_column('shopify_order_items', 'line_item_id'):
        op.add_column('shopify_order_items', sa.Column('line_item_id', sa.BigInteger(), nullable=True))
        op.create_index('ix_shopify_order_items_line_item_id', 'shopify_order_items', ['line_item_id'])

    if not _has_table('shopify_refund_line_items'):
        op.create_table(
            'shopify_refund_line_items',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('shopify_refund_id', sa.BigInteger(), nullable=False, index=True),
            sa.Column('shopify_order_id', sa.BigInteger(), nullable=False, index=True),
            sa.Column('line_item_id', sa.BigInteger(), nullable=True, index=True),
            sa.Column('shopify_product_id', sa.BigInteger(), nullable=True, index=True),
            sa.Column('sku', sa.String(), nullable=True, index=True),
            sa.Column('quantity', sa.Integer(), nullable=False, server_default='0'),
            sa.Column('subtotal', sa.Numeric(10, 2), nullable=False, server_default='0'),
            sa.Column('total_tax', sa.Numeric(10, 2), nullable=False, server_default='0'),
            sa.Column('created_at', sa.DateTime(), nullable=True, index=True),
            sa.Column('processed_at', sa.DateTime(), nullable=True),
            sa.Column('synced_at', sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    if _has_table('shopify_refund_line_items'):
        op.drop_table('shopify_refund_line_items')

    if _has_table('shopify_order_items') and _has_column('shopify_order_items', 'line_item_id'):
        op.drop_index('ix_shopify_order_items_line_item_id', table_name='shopify_order_items')
        op.drop_column('shopify_order_items', 'line_item_id')
