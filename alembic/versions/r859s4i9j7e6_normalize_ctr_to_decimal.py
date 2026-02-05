"""Normalize CTR from percentage (0-100) to decimal (0-1) in search_console tables

CTR was stored as percentage values (e.g. 11.61 for 11.61%) but should be
stored as decimals (e.g. 0.1161) to match the Google Search Console API
format and prevent double-multiplication bugs in the service layer.

Revision ID: r859s4i9j7e6
Revises: q748r3h8i6d5
Create Date: 2026-01-31

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'r859s4i9j7e6'
down_revision: Union[str, None] = 'q748r3h8i6d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Convert CTR from percentage (0-100) to decimal (0-1)."""
    # search_console_queries: divide ctr by 100
    op.execute(
        "UPDATE search_console_queries SET ctr = ctr / 100.0 WHERE ctr IS NOT NULL AND ctr > 1.0"
    )
    # Handle edge case: ctr = 1.0 means 100% (clicks == impressions), keep as 1.0
    # Values already <= 1.0 are either 0 or already decimal — leave unchanged

    # search_console_pages: divide ctr by 100
    op.execute(
        "UPDATE search_console_pages SET ctr = ctr / 100.0 WHERE ctr IS NOT NULL AND ctr > 1.0"
    )


def downgrade() -> None:
    """Convert CTR back from decimal (0-1) to percentage (0-100)."""
    op.execute(
        "UPDATE search_console_queries SET ctr = ctr * 100.0 WHERE ctr IS NOT NULL AND ctr <= 1.0 AND ctr > 0"
    )
    op.execute(
        "UPDATE search_console_pages SET ctr = ctr * 100.0 WHERE ctr IS NOT NULL AND ctr <= 1.0 AND ctr > 0"
    )
