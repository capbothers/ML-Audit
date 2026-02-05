"""
Product Cost Data Model

Stores product cost data from Google Sheets (supplier pricing).
Used by Profitability module to calculate true margins.
"""
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, Boolean, Text
from datetime import datetime, date
from decimal import Decimal

from app.models.base import Base


class ProductCost(Base):
    """
    Product cost data from supplier pricing sheet

    Synced from Google Sheets containing:
    - Supplier costs (nett nett cost after all discounts)
    - RRP (recommended retail price)
    - Minimum margins
    - Special/promo pricing

    This is THE source of truth for product costs.
    """
    __tablename__ = "product_costs"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    vendor_sku = Column(String, unique=True, index=True, nullable=False)  # PRIMARY LOOKUP KEY
    vendor = Column(String, index=True, nullable=True)  # Supplier / brand
    description = Column(String)  # Product name
    ean = Column(String, index=True, nullable=True)  # Barcode
    item_category = Column(String, index=True, nullable=True)  # Product category

    # Pricing
    rrp_inc_gst = Column(Numeric(10, 2), nullable=True)  # Recommended retail price
    invoice_price_inc_gst = Column(Numeric(10, 2), nullable=True)  # Supplier invoice price

    # Special/promo pricing
    special_cost_inc_gst = Column(Numeric(10, 2), nullable=True)  # Promo cost (if active)
    special_end_date = Column(Date, nullable=True)  # When promo expires
    has_active_special = Column(Boolean, default=False, index=True)  # Computed field

    # TRUE COST - This is what we use for profitability calculations
    nett_nett_cost_inc_gst = Column(Numeric(10, 2), nullable=True)  # After ALL discounts (nullable for partial data)

    # Cost breakdown (for transparency)
    discount = Column(Numeric(10, 2), nullable=True)
    additional_discount = Column(Numeric(10, 2), nullable=True)
    extra_discount = Column(Numeric(10, 2), nullable=True)
    rebate = Column(Numeric(10, 2), nullable=True)
    extra = Column(Numeric(10, 2), nullable=True)
    settlement = Column(Numeric(10, 2), nullable=True)
    crf = Column(Numeric(10, 2), nullable=True)
    loyalty = Column(Numeric(10, 2), nullable=True)
    advertising = Column(Numeric(10, 2), nullable=True)
    timed_settlement_fee = Column(Numeric(10, 2), nullable=True)
    other = Column(Numeric(10, 2), nullable=True)

    # Margin controls
    min_margin_pct = Column(Numeric(10, 2), nullable=True)  # Minimum margin % (alert if below)
    minimum_price = Column(Numeric(10, 2), nullable=True)  # Floor price
    discount_off_rrp_pct = Column(Numeric(10, 2), nullable=True)  # Max discount allowed

    # Policy overrides
    do_not_follow = Column(Boolean, default=False)
    set_price = Column(Numeric(10, 2), nullable=True)
    comments = Column(Text, nullable=True)

    # Tax status
    gst_free = Column(Boolean, default=False)  # Is this product GST-free?

    # Timestamps
    last_synced = Column(DateTime, default=datetime.utcnow, index=True)
    last_updated = Column(DateTime, nullable=True)  # When cost last changed

    def get_active_cost(self) -> Decimal:
        """
        Get the currently active cost

        Returns special cost if active, otherwise nett nett cost
        """
        if self.has_active_special and self.special_cost_inc_gst:
            return self.special_cost_inc_gst

        return self.nett_nett_cost_inc_gst

    def calculate_margin(self, selling_price: Decimal) -> Decimal:
        """
        Calculate margin percentage for a given selling price

        Args:
            selling_price: Price product is sold at

        Returns:
            Margin percentage
        """
        cost = self.get_active_cost()

        if not selling_price or selling_price == 0:
            return Decimal(0)

        margin = ((selling_price - cost) / selling_price) * 100

        return margin

    def is_below_minimum_margin(self, selling_price: Decimal) -> bool:
        """
        Check if selling price is below minimum margin

        Args:
            selling_price: Price product is sold at

        Returns:
            True if below minimum margin
        """
        if not self.min_margin_pct:
            return False

        margin = self.calculate_margin(selling_price)

        return margin < self.min_margin_pct

    def update_special_status(self):
        """
        Update has_active_special flag based on special_end_date

        Should be called during sync to keep status current
        """
        if self.special_end_date and self.special_cost_inc_gst:
            self.has_active_special = self.special_end_date >= date.today()
        else:
            self.has_active_special = False
