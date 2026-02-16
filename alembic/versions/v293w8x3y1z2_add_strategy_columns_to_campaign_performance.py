"""Add strategy-aware decision layer columns to campaign_performance

Adds 6 nullable columns for deterministic campaign strategy classification,
composite decision scoring, and dual-status decisions.

Revision ID: v293w8x3y1z2
Revises: t071u6v1w9
Create Date: 2026-02-16
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision: str = 'v293w8x3y1z2'
down_revision: Union[str, None] = 't071u6v1w9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table, column):
    """Check if column already exists (for idempotent migration)."""
    bind = op.get_bind()
    insp = sa_inspect(bind)
    columns = [c['name'] for c in insp.get_columns(table)]
    return column in columns


def upgrade() -> None:
    cols = [
        ('strategy_type', sa.String()),
        ('decision_score', sa.Integer()),
        ('short_term_status', sa.String()),
        ('strategic_value', sa.String()),
        ('strategy_action', sa.String()),
        ('strategy_confidence', sa.String()),
    ]
    for name, col_type in cols:
        if not _has_column('campaign_performance', name):
            op.add_column('campaign_performance', sa.Column(name, col_type, nullable=True))

    # Indexes (IF NOT EXISTS handled by SQLite natively via create_index)
    try:
        op.create_index('ix_campaign_performance_strategy_type', 'campaign_performance', ['strategy_type'])
    except Exception:
        pass  # Index already exists
    try:
        op.create_index('ix_campaign_performance_strategy_action', 'campaign_performance', ['strategy_action'])
    except Exception:
        pass  # Index already exists


def downgrade() -> None:
    try:
        op.drop_index('ix_campaign_performance_strategy_action', table_name='campaign_performance')
    except Exception:
        pass
    try:
        op.drop_index('ix_campaign_performance_strategy_type', table_name='campaign_performance')
    except Exception:
        pass

    for col in ['strategy_confidence', 'strategy_action', 'strategic_value',
                'short_term_status', 'decision_score', 'strategy_type']:
        try:
            op.drop_column('campaign_performance', col)
        except Exception:
            pass
