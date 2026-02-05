"""Add ML intelligence tables

Revision ID: n526o1f6g4b3
Revises: 3babfbd856fb
Create Date: 2026-01-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'n526o1f6g4b3'
down_revision: Union[str, None] = '3babfbd856fb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ml_forecasts
    op.create_table(
        'ml_forecasts',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('metric', sa.String(), nullable=False),
        sa.Column('horizon_days', sa.Integer(), nullable=False),
        sa.Column('predicted_value', sa.Float(), nullable=False),
        sa.Column('lower_bound', sa.Float(), nullable=True),
        sa.Column('upper_bound', sa.Float(), nullable=True),
        sa.Column('model_type', sa.String(), nullable=False),
        sa.Column('training_window_days', sa.Integer(), default=90),
        sa.Column('generated_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('date', 'metric', 'generated_at', name='uq_ml_forecast_date_metric_gen'),
    )
    op.create_index('ix_ml_forecasts_id', 'ml_forecasts', ['id'])
    op.create_index('ix_ml_forecasts_date', 'ml_forecasts', ['date'])
    op.create_index('ix_ml_forecasts_metric', 'ml_forecasts', ['metric'])
    op.create_index('ix_ml_forecasts_generated_at', 'ml_forecasts', ['generated_at'])

    # ml_anomalies
    op.create_table(
        'ml_anomalies',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('metric', sa.String(), nullable=False),
        sa.Column('actual_value', sa.Float(), nullable=False),
        sa.Column('expected_value', sa.Float(), nullable=False),
        sa.Column('deviation_pct', sa.Float(), nullable=False),
        sa.Column('z_score', sa.Float(), nullable=False),
        sa.Column('direction', sa.String(), nullable=False),
        sa.Column('severity', sa.String(), nullable=False),
        sa.Column('baseline_window', sa.Integer(), nullable=False),
        sa.Column('is_acknowledged', sa.Boolean(), default=False),
        sa.Column('acknowledged_at', sa.DateTime(), nullable=True),
        sa.Column('generated_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('date', 'metric', 'baseline_window', name='uq_ml_anomaly_date_metric_window'),
    )
    op.create_index('ix_ml_anomalies_id', 'ml_anomalies', ['id'])
    op.create_index('ix_ml_anomalies_date', 'ml_anomalies', ['date'])
    op.create_index('ix_ml_anomalies_metric', 'ml_anomalies', ['metric'])
    op.create_index('ix_ml_anomalies_severity', 'ml_anomalies', ['severity'])
    op.create_index('ix_ml_anomalies_is_acknowledged', 'ml_anomalies', ['is_acknowledged'])
    op.create_index('ix_ml_anomalies_generated_at', 'ml_anomalies', ['generated_at'])

    # ml_inventory_suggestions
    op.create_table(
        'ml_inventory_suggestions',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('sku', sa.String(), nullable=False),
        sa.Column('brand', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('units_on_hand', sa.Integer(), nullable=False),
        sa.Column('daily_sales_velocity', sa.Float(), nullable=False),
        sa.Column('velocity_trend', sa.String(), nullable=True),
        sa.Column('days_of_cover', sa.Float(), nullable=False),
        sa.Column('suggestion', sa.String(), nullable=False),
        sa.Column('reorder_quantity', sa.Integer(), nullable=True),
        sa.Column('urgency', sa.String(), nullable=False),
        sa.Column('generated_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('sku', 'generated_at', name='uq_ml_inventory_sku_gen'),
    )
    op.create_index('ix_ml_inventory_suggestions_id', 'ml_inventory_suggestions', ['id'])
    op.create_index('ix_ml_inventory_suggestions_sku', 'ml_inventory_suggestions', ['sku'])
    op.create_index('ix_ml_inventory_suggestions_brand', 'ml_inventory_suggestions', ['brand'])
    op.create_index('ix_ml_inventory_suggestions_suggestion', 'ml_inventory_suggestions', ['suggestion'])
    op.create_index('ix_ml_inventory_suggestions_urgency', 'ml_inventory_suggestions', ['urgency'])
    op.create_index('ix_ml_inventory_suggestions_generated_at', 'ml_inventory_suggestions', ['generated_at'])


def downgrade() -> None:
    op.drop_table('ml_inventory_suggestions')
    op.drop_table('ml_anomalies')
    op.drop_table('ml_forecasts')
