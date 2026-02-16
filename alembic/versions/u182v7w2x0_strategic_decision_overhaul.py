"""Add decision-layer columns to strategic intelligence tables

Revision ID: u182v7w2x0
Revises: None (standalone — safe to run on any head)
"""
from alembic import op
import sqlalchemy as sa

revision = 'u182v7w2x0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ── BriefRecommendation: 14 new columns ──
    with op.batch_alter_table('strategic_brief_recommendations') as batch:
        batch.add_column(sa.Column('due_date', sa.Date(), nullable=True))
        batch.add_column(sa.Column('priority_score', sa.Float(), server_default='0'))
        batch.add_column(sa.Column('urgency_weight', sa.Float(), server_default='1.0'))
        batch.add_column(sa.Column('data_as_of', sa.JSON(), nullable=True))
        batch.add_column(sa.Column('dedup_hash', sa.String(32), nullable=True))
        batch.add_column(sa.Column('is_cross_functional', sa.Boolean(), server_default='1'))
        batch.add_column(sa.Column('baseline_metric_name', sa.String(), nullable=True))
        batch.add_column(sa.Column('baseline_metric_value', sa.Float(), nullable=True))
        batch.add_column(sa.Column('target_metric_value', sa.Float(), nullable=True))
        batch.add_column(sa.Column('impact_7d', sa.Float(), nullable=True))
        batch.add_column(sa.Column('impact_30d', sa.Float(), nullable=True))

    # Index for dedup lookups
    op.create_index('ix_brief_rec_dedup_hash', 'strategic_brief_recommendations', ['dedup_hash'])

    # ── StrategicBrief: 3 new columns ──
    with op.batch_alter_table('strategic_briefs') as batch:
        batch.add_column(sa.Column('is_degraded', sa.Boolean(), server_default='0'))
        batch.add_column(sa.Column('stale_modules', sa.JSON(), nullable=True))
        batch.add_column(sa.Column('module_freshness', sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table('strategic_briefs') as batch:
        batch.drop_column('module_freshness')
        batch.drop_column('stale_modules')
        batch.drop_column('is_degraded')

    op.drop_index('ix_brief_rec_dedup_hash', table_name='strategic_brief_recommendations')

    with op.batch_alter_table('strategic_brief_recommendations') as batch:
        batch.drop_column('impact_30d')
        batch.drop_column('impact_7d')
        batch.drop_column('target_metric_value')
        batch.drop_column('baseline_metric_value')
        batch.drop_column('baseline_metric_name')
        batch.drop_column('is_cross_functional')
        batch.drop_column('dedup_hash')
        batch.drop_column('data_as_of')
        batch.drop_column('urgency_weight')
        batch.drop_column('priority_score')
        batch.drop_column('due_date')
