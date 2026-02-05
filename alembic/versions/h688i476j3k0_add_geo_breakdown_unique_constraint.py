"""add_geo_breakdown_unique_constraint

Revision ID: h688i476j3k0
Revises: g577h365i2j9
Create Date: 2026-01-27

Adds unique constraint on (date, country, region, city) to ga4_geo_breakdown
to prevent duplicate entries during upserts.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'h688i476j3k0'
down_revision: Union[str, Sequence[str], None] = 'g577h365i2j9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add unique constraint on (date, country, region, city)."""
    # SQLite requires batch mode for adding constraints to existing tables
    with op.batch_alter_table('ga4_geo_breakdown', schema=None) as batch_op:
        batch_op.create_unique_constraint(
            'uq_ga4_geo_date_country_region_city',
            ['date', 'country', 'region', 'city']
        )


def downgrade() -> None:
    """Remove unique constraint."""
    with op.batch_alter_table('ga4_geo_breakdown', schema=None) as batch_op:
        batch_op.drop_constraint(
            'uq_ga4_geo_date_country_region_city',
            type_='unique'
        )
