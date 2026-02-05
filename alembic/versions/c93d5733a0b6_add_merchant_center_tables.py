"""Add merchant center tables

Revision ID: c93d5733a0b6
Revises: b92c4622f9e5
Create Date: 2026-01-27 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c93d5733a0b6'
down_revision: Union[str, None] = 'b92c4622f9e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create merchant_center_product_statuses table
    op.create_table(
        'merchant_center_product_statuses',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('product_id', sa.String(), nullable=False),
        sa.Column('offer_id', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('approval_status', sa.String(), nullable=False),
        sa.Column('has_issues', sa.Boolean(), default=False),
        sa.Column('issue_count', sa.Integer(), default=0),
        sa.Column('critical_issue_count', sa.Integer(), default=0),
        sa.Column('synced_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_merchant_center_product_statuses_product_id', 'merchant_center_product_statuses', ['product_id'])
    op.create_index('ix_merchant_center_product_statuses_snapshot_date', 'merchant_center_product_statuses', ['snapshot_date'])
    op.create_index('ix_merchant_center_product_statuses_approval_status', 'merchant_center_product_statuses', ['approval_status'])
    op.create_index('ix_merchant_center_product_statuses_has_issues', 'merchant_center_product_statuses', ['has_issues'])
    op.create_index('ix_mc_product_date', 'merchant_center_product_statuses', ['product_id', 'snapshot_date'])

    # Create merchant_center_disapprovals table
    op.create_table(
        'merchant_center_disapprovals',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('product_id', sa.String(), nullable=False),
        sa.Column('offer_id', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('snapshot_date', sa.Date(), nullable=False),
        sa.Column('issue_code', sa.String(), nullable=False),
        sa.Column('issue_severity', sa.String(), nullable=True),
        sa.Column('issue_description', sa.String(), nullable=True),
        sa.Column('issue_detail', sa.Text(), nullable=True),
        sa.Column('issue_attribute', sa.String(), nullable=True),
        sa.Column('issue_destination', sa.String(), nullable=True),
        sa.Column('documentation_url', sa.String(), nullable=True),
        sa.Column('first_seen_date', sa.Date(), nullable=True),
        sa.Column('is_resolved', sa.Boolean(), default=False),
        sa.Column('resolved_date', sa.Date(), nullable=True),
        sa.Column('synced_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_merchant_center_disapprovals_product_id', 'merchant_center_disapprovals', ['product_id'])
    op.create_index('ix_merchant_center_disapprovals_offer_id', 'merchant_center_disapprovals', ['offer_id'])
    op.create_index('ix_merchant_center_disapprovals_snapshot_date', 'merchant_center_disapprovals', ['snapshot_date'])
    op.create_index('ix_merchant_center_disapprovals_issue_code', 'merchant_center_disapprovals', ['issue_code'])
    op.create_index('ix_merchant_center_disapprovals_issue_severity', 'merchant_center_disapprovals', ['issue_severity'])
    op.create_index('ix_merchant_center_disapprovals_first_seen_date', 'merchant_center_disapprovals', ['first_seen_date'])
    op.create_index('ix_merchant_center_disapprovals_is_resolved', 'merchant_center_disapprovals', ['is_resolved'])
    op.create_index('ix_mc_disapproval_product_issue_date', 'merchant_center_disapprovals', ['product_id', 'issue_code', 'snapshot_date'])

    # Create merchant_center_account_statuses table
    op.create_table(
        'merchant_center_account_statuses',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('snapshot_date', sa.Date(), nullable=False, unique=True),
        sa.Column('total_products', sa.Integer(), default=0),
        sa.Column('approved_count', sa.Integer(), default=0),
        sa.Column('disapproved_count', sa.Integer(), default=0),
        sa.Column('pending_count', sa.Integer(), default=0),
        sa.Column('expiring_count', sa.Integer(), default=0),
        sa.Column('approval_rate', sa.Float(), nullable=True),
        sa.Column('account_issue_count', sa.Integer(), default=0),
        sa.Column('account_issues', sa.JSON(), nullable=True),
        sa.Column('website_claimed', sa.Boolean(), default=False),
        sa.Column('synced_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_merchant_center_account_statuses_snapshot_date', 'merchant_center_account_statuses', ['snapshot_date'])


def downgrade() -> None:
    op.drop_table('merchant_center_account_statuses')
    op.drop_table('merchant_center_disapprovals')
    op.drop_table('merchant_center_product_statuses')
