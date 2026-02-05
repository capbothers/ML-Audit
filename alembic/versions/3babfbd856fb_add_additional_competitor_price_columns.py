"""add_additional_competitor_price_columns

Revision ID: 3babfbd856fb
Revises: m415n0e5f3a2
Create Date: 2026-01-28 22:56:21.953659

Adds 9 new competitor price columns to competitive_pricing table:
- agcequipment
- berloniappliances
- eands
- plumbingsales
- powerland
- saappliancewarehouse
- samedayhotwaterservice
- shireskylights
- voguespas
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3babfbd856fb'
down_revision: Union[str, Sequence[str], None] = 'm415n0e5f3a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add new competitor price columns to competitive_pricing table."""
    op.add_column('competitive_pricing', sa.Column('price_agcequipment', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_berloniapp', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_eands', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_plumbingsales', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_powerland', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_saappliances', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_sameday', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_shire', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('competitive_pricing', sa.Column('price_vogue', sa.Numeric(precision=10, scale=2), nullable=True))


def downgrade() -> None:
    """Remove new competitor price columns from competitive_pricing table."""
    op.drop_column('competitive_pricing', 'price_vogue')
    op.drop_column('competitive_pricing', 'price_shire')
    op.drop_column('competitive_pricing', 'price_sameday')
    op.drop_column('competitive_pricing', 'price_saappliances')
    op.drop_column('competitive_pricing', 'price_powerland')
    op.drop_column('competitive_pricing', 'price_plumbingsales')
    op.drop_column('competitive_pricing', 'price_eands')
    op.drop_column('competitive_pricing', 'price_berloniapp')
    op.drop_column('competitive_pricing', 'price_agcequipment')
