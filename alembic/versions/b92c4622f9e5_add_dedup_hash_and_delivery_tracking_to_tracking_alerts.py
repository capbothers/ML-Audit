"""add_dedup_hash_and_delivery_tracking_to_tracking_alerts

Revision ID: b92c4622f9e5
Revises: a84b3511e8d4
Create Date: 2026-01-26

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b92c4622f9e5'
down_revision: Union[str, Sequence[str], None] = 'a84b3511e8d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add dedup_hash and delivery tracking columns to tracking_alerts table."""
    # Deduplication hash for preventing duplicate alerts
    op.add_column('tracking_alerts', sa.Column('dedup_hash', sa.String(32), nullable=True))
    op.create_index('ix_tracking_alerts_dedup_hash', 'tracking_alerts', ['dedup_hash'])

    # Delivery attempt tracking for audit trail
    op.add_column('tracking_alerts', sa.Column('delivery_attempts', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('tracking_alerts', sa.Column('delivery_total_delay_seconds', sa.Float(), nullable=False, server_default='0.0'))
    op.add_column('tracking_alerts', sa.Column('delivery_results', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Remove dedup_hash and delivery tracking columns from tracking_alerts table."""
    op.drop_column('tracking_alerts', 'delivery_results')
    op.drop_column('tracking_alerts', 'delivery_total_delay_seconds')
    op.drop_column('tracking_alerts', 'delivery_attempts')
    op.drop_index('ix_tracking_alerts_dedup_hash', table_name='tracking_alerts')
    op.drop_column('tracking_alerts', 'dedup_hash')
