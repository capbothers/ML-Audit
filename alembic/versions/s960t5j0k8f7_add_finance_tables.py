"""Add business_expenses, monthly_pl tables and fully-loaded columns on campaign_performance

Revision ID: s960t5j0k8f7
Revises: r859s4i9j7e6
Create Date: 2026-02-03
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers
revision: str = 's960t5j0k8f7'
down_revision: Union[str, None] = 'r859s4i9j7e6'
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
    # ── business_expenses table ──
    if not _has_table('business_expenses'):
        op.create_table(
            'business_expenses',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('month', sa.Date(), nullable=False, index=True),
            sa.Column('category', sa.String(), nullable=False, index=True),
            sa.Column('description', sa.String(), nullable=False),
            sa.Column('amount', sa.Numeric(12, 2), nullable=False),
            sa.Column('is_recurring', sa.Boolean(), server_default='1'),
            sa.Column('notes', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now()),
        )

    # ── monthly_pl table ──
    if not _has_table('monthly_pl'):
        op.create_table(
            'monthly_pl',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('month', sa.Date(), nullable=False, unique=True, index=True),
            # Revenue
            sa.Column('gross_revenue', sa.Numeric(12, 2), server_default='0'),
            sa.Column('refunds', sa.Numeric(12, 2), server_default='0'),
            sa.Column('net_revenue', sa.Numeric(12, 2), server_default='0'),
            # COGS
            sa.Column('cogs', sa.Numeric(12, 2), server_default='0'),
            # Gross margin
            sa.Column('gross_margin', sa.Numeric(12, 2), server_default='0'),
            sa.Column('gross_margin_pct', sa.Numeric(5, 2), nullable=True),
            # Operating expenses
            sa.Column('ad_spend', sa.Numeric(12, 2), server_default='0'),
            sa.Column('payroll', sa.Numeric(12, 2), server_default='0'),
            sa.Column('rent', sa.Numeric(12, 2), server_default='0'),
            sa.Column('shipping', sa.Numeric(12, 2), server_default='0'),
            sa.Column('utilities', sa.Numeric(12, 2), server_default='0'),
            sa.Column('insurance', sa.Numeric(12, 2), server_default='0'),
            sa.Column('software', sa.Numeric(12, 2), server_default='0'),
            sa.Column('marketing_other', sa.Numeric(12, 2), server_default='0'),
            sa.Column('professional_services', sa.Numeric(12, 2), server_default='0'),
            sa.Column('other_expenses', sa.Numeric(12, 2), server_default='0'),
            sa.Column('total_expenses', sa.Numeric(12, 2), server_default='0'),
            # Profit
            sa.Column('operating_profit', sa.Numeric(12, 2), server_default='0'),
            sa.Column('operating_margin_pct', sa.Numeric(5, 2), nullable=True),
            sa.Column('net_profit', sa.Numeric(12, 2), server_default='0'),
            sa.Column('net_margin_pct', sa.Numeric(5, 2), nullable=True),
            # Orders
            sa.Column('total_orders', sa.Integer(), server_default='0'),
            sa.Column('avg_order_value', sa.Numeric(10, 2), nullable=True),
            sa.Column('overhead_per_order', sa.Numeric(10, 2), nullable=True),
            # Timestamps
            sa.Column('generated_at', sa.DateTime(), server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now()),
        )

    # ── Add fully-loaded columns to campaign_performance ──
    if _has_table('campaign_performance'):
        if not _has_column('campaign_performance', 'allocated_overhead'):
            op.add_column('campaign_performance',
                sa.Column('allocated_overhead', sa.Numeric(12, 2), nullable=True))
        if not _has_column('campaign_performance', 'fully_loaded_profit'):
            op.add_column('campaign_performance',
                sa.Column('fully_loaded_profit', sa.Numeric(12, 2), nullable=True))
        if not _has_column('campaign_performance', 'fully_loaded_roas'):
            op.add_column('campaign_performance',
                sa.Column('fully_loaded_roas', sa.Float(), nullable=True))
        if not _has_column('campaign_performance', 'is_profitable_fully_loaded'):
            op.add_column('campaign_performance',
                sa.Column('is_profitable_fully_loaded', sa.Boolean(), nullable=True))


def downgrade() -> None:
    # Remove campaign_performance columns
    if _has_table('campaign_performance'):
        if _has_column('campaign_performance', 'is_profitable_fully_loaded'):
            op.drop_column('campaign_performance', 'is_profitable_fully_loaded')
        if _has_column('campaign_performance', 'fully_loaded_roas'):
            op.drop_column('campaign_performance', 'fully_loaded_roas')
        if _has_column('campaign_performance', 'fully_loaded_profit'):
            op.drop_column('campaign_performance', 'fully_loaded_profit')
        if _has_column('campaign_performance', 'allocated_overhead'):
            op.drop_column('campaign_performance', 'allocated_overhead')

    if _has_table('monthly_pl'):
        op.drop_table('monthly_pl')

    if _has_table('business_expenses'):
        op.drop_table('business_expenses')
