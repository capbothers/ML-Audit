"""
Google Merchant Center Data Models

Stores product status snapshots and disapproval history for tracking
product health over time.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, Date, Index
from datetime import datetime

from app.models.base import Base


class MerchantCenterProductStatus(Base):
    """
    Daily snapshot of Merchant Center product status.
    Tracks approval status changes over time for historical analysis.
    """
    __tablename__ = "merchant_center_product_statuses"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(String, index=True, nullable=False)
    # Format: online:en:AU:shopify_AU_123456789
    offer_id = Column(String, index=True, nullable=True)
    # SKU from product feed
    title = Column(String, nullable=True)

    # Snapshot date
    snapshot_date = Column(Date, index=True, nullable=False)

    # Status (for Shopping destination)
    approval_status = Column(String, index=True, nullable=False)
    # Values: approved, disapproved, pending

    # Issue summary
    has_issues = Column(Boolean, default=False, index=True)
    issue_count = Column(Integer, default=0)
    critical_issue_count = Column(Integer, default=0)
    # Count of issues that prevent product from showing

    # Timestamp
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Composite index for efficient queries
    __table_args__ = (
        Index('ix_mc_product_date', 'product_id', 'snapshot_date'),
    )

    def __repr__(self):
        return f"<MerchantCenterProductStatus {self.product_id} - {self.snapshot_date} - {self.approval_status}>"


class MerchantCenterDisapproval(Base):
    """
    Historical record of product disapprovals with issue details.
    One record per product per issue per day.
    """
    __tablename__ = "merchant_center_disapprovals"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(String, index=True, nullable=False)
    offer_id = Column(String, index=True, nullable=True)
    title = Column(String, nullable=True)

    # Snapshot date
    snapshot_date = Column(Date, index=True, nullable=False)

    # Issue details
    issue_code = Column(String, index=True, nullable=False)
    # e.g., "missing_gtin", "image_too_small", "price_mismatch"

    issue_severity = Column(String, index=True, nullable=True)
    # e.g., "disapproved", "demoted", "unaffected"

    issue_description = Column(String, nullable=True)
    issue_detail = Column(Text, nullable=True)
    issue_attribute = Column(String, nullable=True)
    # The attribute causing the issue (e.g., "gtin", "image_link")

    issue_destination = Column(String, nullable=True)
    # e.g., "Shopping", "DisplayAds"

    documentation_url = Column(String, nullable=True)

    # Status tracking
    first_seen_date = Column(Date, index=True, nullable=True)
    # Date this issue was first detected for this product

    is_resolved = Column(Boolean, default=False, index=True)
    resolved_date = Column(Date, nullable=True)

    # Timestamp
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Composite index for efficient queries
    __table_args__ = (
        Index('ix_mc_disapproval_product_issue_date', 'product_id', 'issue_code', 'snapshot_date'),
    )

    def __repr__(self):
        return f"<MerchantCenterDisapproval {self.product_id} - {self.issue_code} - {self.snapshot_date}>"


class MerchantCenterAccountStatus(Base):
    """
    Daily snapshot of Merchant Center account-level status.
    Tracks account issues and product statistics over time.
    """
    __tablename__ = "merchant_center_account_statuses"

    id = Column(Integer, primary_key=True, index=True)

    # Snapshot date
    snapshot_date = Column(Date, index=True, nullable=False, unique=True)

    # Product counts
    total_products = Column(Integer, default=0)
    approved_count = Column(Integer, default=0)
    disapproved_count = Column(Integer, default=0)
    pending_count = Column(Integer, default=0)
    expiring_count = Column(Integer, default=0)

    # Rates
    approval_rate = Column(Float, nullable=True)
    # approved_count / total_products * 100

    # Account issues
    account_issue_count = Column(Integer, default=0)
    account_issues = Column(JSON, nullable=True)
    # List of account-level issues

    # Website status
    website_claimed = Column(Boolean, default=False)

    # Timestamp
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        rate = f"{self.approval_rate:.1f}%" if self.approval_rate is not None else "N/A"
        return f"<MerchantCenterAccountStatus {self.snapshot_date} - {rate} approved>"
