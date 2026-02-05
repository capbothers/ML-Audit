"""add_ga4_segmentation_tables

Revision ID: g577h365i2j9
Revises: f466g254h1i8
Create Date: 2026-01-27

Adds GA4 segmentation tables for:
- Device breakdown (desktop, mobile, tablet)
- Geographic breakdown (country, region, city)
- User type breakdown (new vs returning)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'g577h365i2j9'
down_revision: Union[str, Sequence[str], None] = 'f466g254h1i8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create ga4_device_breakdown, ga4_geo_breakdown, ga4_user_type tables."""

    # GA4 Device Breakdown
    op.create_table(
        'ga4_device_breakdown',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('device_category', sa.String(), nullable=False),

        # Traffic metrics
        sa.Column('sessions', sa.Integer(), server_default='0'),
        sa.Column('active_users', sa.Integer(), server_default='0'),
        sa.Column('new_users', sa.Integer(), server_default='0'),

        # Engagement
        sa.Column('engaged_sessions', sa.Integer(), server_default='0'),
        sa.Column('bounce_rate', sa.Float(), nullable=True),
        sa.Column('avg_session_duration', sa.Float(), nullable=True),

        # Conversions
        sa.Column('conversions', sa.Integer(), server_default='0'),
        sa.Column('total_revenue', sa.Numeric(10, 2), server_default='0'),

        # Metadata
        sa.Column('synced_at', sa.DateTime(), nullable=False),

        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('date', 'device_category', name='uq_ga4_device_date_category')
    )
    op.create_index('ix_ga4_device_breakdown_date', 'ga4_device_breakdown', ['date'])
    op.create_index('ix_ga4_device_breakdown_category', 'ga4_device_breakdown', ['device_category'])

    # GA4 Geo Breakdown
    op.create_table(
        'ga4_geo_breakdown',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('country', sa.String(), nullable=False),
        sa.Column('region', sa.String(), nullable=True),
        sa.Column('city', sa.String(), nullable=True),

        # Traffic metrics
        sa.Column('sessions', sa.Integer(), server_default='0'),
        sa.Column('active_users', sa.Integer(), server_default='0'),
        sa.Column('new_users', sa.Integer(), server_default='0'),

        # Engagement
        sa.Column('engaged_sessions', sa.Integer(), server_default='0'),
        sa.Column('bounce_rate', sa.Float(), nullable=True),

        # Conversions
        sa.Column('conversions', sa.Integer(), server_default='0'),
        sa.Column('total_revenue', sa.Numeric(10, 2), server_default='0'),

        # Metadata
        sa.Column('synced_at', sa.DateTime(), nullable=False),

        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_ga4_geo_breakdown_date', 'ga4_geo_breakdown', ['date'])
    op.create_index('ix_ga4_geo_breakdown_country', 'ga4_geo_breakdown', ['country'])
    op.create_index('ix_ga4_geo_breakdown_region', 'ga4_geo_breakdown', ['region'])

    # GA4 User Type
    op.create_table(
        'ga4_user_type',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('user_type', sa.String(), nullable=False),

        # Metrics
        sa.Column('users', sa.Integer(), server_default='0'),
        sa.Column('sessions', sa.Integer(), server_default='0'),
        sa.Column('engaged_sessions', sa.Integer(), server_default='0'),
        sa.Column('pageviews', sa.Integer(), server_default='0'),
        sa.Column('avg_session_duration', sa.Float(), nullable=True),

        # Conversions
        sa.Column('conversions', sa.Integer(), server_default='0'),
        sa.Column('total_revenue', sa.Numeric(10, 2), server_default='0'),

        # Metadata
        sa.Column('synced_at', sa.DateTime(), nullable=False),

        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('date', 'user_type', name='uq_ga4_user_type_date')
    )
    op.create_index('ix_ga4_user_type_date', 'ga4_user_type', ['date'])
    op.create_index('ix_ga4_user_type_type', 'ga4_user_type', ['user_type'])


def downgrade() -> None:
    """Drop segmentation tables."""
    # Drop user type
    op.drop_index('ix_ga4_user_type_type', table_name='ga4_user_type')
    op.drop_index('ix_ga4_user_type_date', table_name='ga4_user_type')
    op.drop_table('ga4_user_type')

    # Drop geo breakdown
    op.drop_index('ix_ga4_geo_breakdown_region', table_name='ga4_geo_breakdown')
    op.drop_index('ix_ga4_geo_breakdown_country', table_name='ga4_geo_breakdown')
    op.drop_index('ix_ga4_geo_breakdown_date', table_name='ga4_geo_breakdown')
    op.drop_table('ga4_geo_breakdown')

    # Drop device breakdown
    op.drop_index('ix_ga4_device_breakdown_category', table_name='ga4_device_breakdown')
    op.drop_index('ix_ga4_device_breakdown_date', table_name='ga4_device_breakdown')
    op.drop_table('ga4_device_breakdown')
