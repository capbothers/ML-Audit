"""Merge heads

Revision ID: 466ae289947c
Revises: 75d588541066, c93d5733a0b6
Create Date: 2026-01-27 02:48:09.663446

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '466ae289947c'
down_revision: Union[str, Sequence[str], None] = ('75d588541066', 'c93d5733a0b6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
