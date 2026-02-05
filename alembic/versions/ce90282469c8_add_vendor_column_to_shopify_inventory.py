"""Add vendor column to shopify_inventory

Revision ID: ce90282469c8
Revises: i799j587k4l1
Create Date: 2026-01-28 07:42:16.570739

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'ce90282469c8'
down_revision: Union[str, Sequence[str], None] = 'i799j587k4l1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add vendor column to shopify_inventory table."""
    op.add_column('shopify_inventory', sa.Column('vendor', sa.String(), nullable=True))


def downgrade() -> None:
    """Remove vendor column from shopify_inventory table."""
    op.drop_column('shopify_inventory', 'vendor')
