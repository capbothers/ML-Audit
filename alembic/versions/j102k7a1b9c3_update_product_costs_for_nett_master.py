"""Update product_costs for NETT master sheet

Revision ID: j102k7a1b9c3
Revises: ce90282469c8
Create Date: 2026-01-28 08:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "j102k7a1b9c3"
down_revision: Union[str, Sequence[str], None] = "ce90282469c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("product_costs", sa.Column("vendor", sa.String(), nullable=True))
    op.add_column("product_costs", sa.Column("discount", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("additional_discount", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("extra_discount", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("rebate", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("extra", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("settlement", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("crf", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("loyalty", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("advertising", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("timed_settlement_fee", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("other", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("do_not_follow", sa.Boolean(), nullable=True))
    op.add_column("product_costs", sa.Column("set_price", sa.Numeric(10, 2), nullable=True))
    op.add_column("product_costs", sa.Column("comments", sa.Text(), nullable=True))

    op.create_index(op.f("ix_product_costs_vendor"), "product_costs", ["vendor"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_product_costs_vendor"), table_name="product_costs")
    op.drop_column("product_costs", "comments")
    op.drop_column("product_costs", "set_price")
    op.drop_column("product_costs", "do_not_follow")
    op.drop_column("product_costs", "other")
    op.drop_column("product_costs", "timed_settlement_fee")
    op.drop_column("product_costs", "advertising")
    op.drop_column("product_costs", "loyalty")
    op.drop_column("product_costs", "crf")
    op.drop_column("product_costs", "settlement")
    op.drop_column("product_costs", "extra")
    op.drop_column("product_costs", "rebate")
    op.drop_column("product_costs", "extra_discount")
    op.drop_column("product_costs", "additional_discount")
    op.drop_column("product_costs", "discount")
    op.drop_column("product_costs", "vendor")
