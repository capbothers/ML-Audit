"""Add pricing impact cache table

Optional cache for daily pricing impact analysis results.
The pricing intelligence service computes live from existing tables,
but this cache allows fast retrieval of pre-computed daily snapshots.

Revision ID: p637q2g7h5c4
Revises: n526o1f6g4b3
Create Date: 2026-01-29
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'p637q2g7h5c4'
down_revision: Union[str, None] = 'n526o1f6g4b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'pricing_impact_cache',
        sa.Column('id', sa.Integer(), primary_key=True, index=True),
        sa.Column('analysis_date', sa.Date(), nullable=False, unique=True, index=True),
        sa.Column('sku_sensitivity_data', sa.JSON(), nullable=True),
        sa.Column('brand_impact_data', sa.JSON(), nullable=True),
        sa.Column('unmatchable_data', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table('pricing_impact_cache')
