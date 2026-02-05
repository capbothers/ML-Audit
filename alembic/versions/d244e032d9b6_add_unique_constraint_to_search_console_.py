"""Add unique constraint to search_console_sitemaps.sitemap_url

Revision ID: d244e032d9b6
Revises: 466ae289947c
Create Date: 2026-01-27 02:48:22.907504

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd244e032d9b6'
down_revision: Union[str, Sequence[str], None] = '466ae289947c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_index(op.f('ix_search_console_sitemaps_sitemap_url'), table_name='search_console_sitemaps')
    op.create_index(
        op.f('ix_search_console_sitemaps_sitemap_url'),
        'search_console_sitemaps',
        ['sitemap_url'],
        unique=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_search_console_sitemaps_sitemap_url'), table_name='search_console_sitemaps')
    op.create_index(
        op.f('ix_search_console_sitemaps_sitemap_url'),
        'search_console_sitemaps',
        ['sitemap_url'],
        unique=False,
    )
