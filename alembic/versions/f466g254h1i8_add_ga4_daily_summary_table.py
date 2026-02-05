"""add_ga4_daily_summary_table

Revision ID: f466g254h1i8
Revises: e355f143e0c7
Create Date: 2026-01-27

Adds the GA4DailySummary table for storing daily site-wide metrics.
This provides a dedicated table for aggregate daily metrics instead of
storing them awkwardly in GA4TrafficSource with source='(all)'.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f466g254h1i8'
down_revision: Union[str, Sequence[str], None] = 'e355f143e0c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create ga4_daily_summary table with all metrics."""
    op.create_table(
        'ga4_daily_summary',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),

        # Core traffic metrics
        sa.Column('active_users', sa.Integer(), server_default='0'),
        sa.Column('new_users', sa.Integer(), server_default='0'),
        sa.Column('returning_users', sa.Integer(), server_default='0'),
        sa.Column('sessions', sa.Integer(), server_default='0'),
        sa.Column('pageviews', sa.Integer(), server_default='0'),

        # Engagement metrics
        sa.Column('engaged_sessions', sa.Integer(), server_default='0'),
        sa.Column('engagement_rate', sa.Float(), nullable=True),
        sa.Column('bounce_rate', sa.Float(), nullable=True),
        sa.Column('avg_session_duration', sa.Float(), nullable=True),
        sa.Column('avg_engagement_duration', sa.Float(), nullable=True),
        sa.Column('pages_per_session', sa.Float(), nullable=True),
        sa.Column('events_per_session', sa.Float(), nullable=True),
        sa.Column('total_events', sa.Integer(), server_default='0'),

        # Conversion summary
        sa.Column('total_conversions', sa.Integer(), server_default='0'),
        sa.Column('total_revenue', sa.Numeric(12, 2), server_default='0'),

        # Metadata
        sa.Column('synced_at', sa.DateTime(), nullable=False),

        sa.PrimaryKeyConstraint('id')
    )

    # Create unique index on date (one row per day)
    op.create_index(
        'ix_ga4_daily_summary_date',
        'ga4_daily_summary',
        ['date'],
        unique=True
    )


def downgrade() -> None:
    """Drop ga4_daily_summary table."""
    op.drop_index('ix_ga4_daily_summary_date', table_name='ga4_daily_summary')
    op.drop_table('ga4_daily_summary')
