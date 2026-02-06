"""
Competitive Pricing Data Model

Stores pricing data from Caprice competitive intelligence exports.
Links to Shopify orders via variant_id for true profitability analysis.

Keeps historical pricing data - one record per product per day.
"""
from sqlalchemy import Column, Integer, String, Numeric, DateTime, Boolean, BigInteger, Date, UniqueConstraint
from datetime import datetime, date
from decimal import Decimal

from app.models.base import Base


class CompetitivePricing(Base):
    """
    Competitive pricing data from Caprice exports

    Contains:
    - Product pricing (RRP, cost, selling price)
    - Competitor prices (24 competitors tracked)
    - Profit margins and alerts

    Keeps history: one record per variant_id per pricing_date
    """
    __tablename__ = "competitive_pricing"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification - links to Shopify
    variant_id = Column(BigInteger, index=True, nullable=False)  # Shopify variant ID
    pricing_date = Column(Date, index=True, nullable=False)  # Date of this pricing snapshot

    # Unique constraint: one record per product per day
    __table_args__ = (
        UniqueConstraint('variant_id', 'pricing_date', name='uq_variant_pricing_date'),
    )
    variant_sku = Column(String, index=True, nullable=True)
    title = Column(String, nullable=True)
    vendor = Column(String, index=True, nullable=True)

    # Pricing rules
    match_rule = Column(String, nullable=True)  # Match rule from Caprice
    set_price = Column(Numeric(10, 2), nullable=True)  # Fixed price if set
    ceiling_price = Column(Numeric(10, 2), nullable=True)  # Maximum price

    # Core pricing
    rrp = Column(Numeric(10, 2), nullable=True)  # Recommended retail price
    current_price = Column(Numeric(10, 2), nullable=True)  # Current Cass price
    minimum_price = Column(Numeric(10, 2), nullable=True)  # Minimum allowed price
    nett_cost = Column(Numeric(10, 2), nullable=True)  # True cost (NETT)

    # Competitive data
    lowest_competitor_price = Column(Numeric(10, 2), nullable=True)
    price_vs_minimum = Column(Numeric(10, 2), nullable=True)  # Lowest - Minimum

    # Profitability
    profit_margin_pct = Column(Numeric(10, 2), nullable=True)  # % Profit Margin
    profit_amount = Column(Numeric(10, 2), nullable=True)  # Profit in $
    discount_off_rrp_pct = Column(Numeric(10, 2), nullable=True)  # % Off RRP

    # Competitor prices (top competitors)
    price_8appliances = Column(Numeric(10, 2), nullable=True)
    price_appliancesonline = Column(Numeric(10, 2), nullable=True)
    price_austpek = Column(Numeric(10, 2), nullable=True)
    price_binglee = Column(Numeric(10, 2), nullable=True)
    price_blueleafbath = Column(Numeric(10, 2), nullable=True)
    price_brandsdirect = Column(Numeric(10, 2), nullable=True)
    price_buildmat = Column(Numeric(10, 2), nullable=True)
    price_cookandbathe = Column(Numeric(10, 2), nullable=True)
    price_designerbathware = Column(Numeric(10, 2), nullable=True)
    price_harveynorman = Column(Numeric(10, 2), nullable=True)
    price_idealbathroom = Column(Numeric(10, 2), nullable=True)
    price_justbathroomware = Column(Numeric(10, 2), nullable=True)
    price_thebluespace = Column(Numeric(10, 2), nullable=True)
    price_wellsons = Column(Numeric(10, 2), nullable=True)
    price_winnings = Column(Numeric(10, 2), nullable=True)

    # Additional competitors (added 2026-01-28)
    price_agcequipment = Column(Numeric(10, 2), nullable=True)
    price_berloniapp = Column(Numeric(10, 2), nullable=True)
    price_eands = Column(Numeric(10, 2), nullable=True)
    price_plumbingsales = Column(Numeric(10, 2), nullable=True)
    price_powerland = Column(Numeric(10, 2), nullable=True)
    price_saappliances = Column(Numeric(10, 2), nullable=True)
    price_sameday = Column(Numeric(10, 2), nullable=True)
    price_shire = Column(Numeric(10, 2), nullable=True)
    price_vogue = Column(Numeric(10, 2), nullable=True)

    # Alerts/flags
    is_losing_money = Column(Boolean, default=False, index=True)  # Profit < 0
    is_below_minimum = Column(Boolean, default=False, index=True)  # Price below minimum
    is_above_rrp = Column(Boolean, default=False, index=True)  # Price above RRP
    has_no_cost = Column(Boolean, default=False, index=True)  # Missing NETT cost

    # Timestamps
    import_date = Column(DateTime, default=datetime.utcnow, index=True)
    source_file = Column(String, nullable=True)  # Which file this came from

    def calculate_flags(self):
        """Update alert flags based on current data"""
        # Losing money
        if self.profit_amount is not None:
            self.is_losing_money = float(self.profit_amount) < 0
        elif self.nett_cost and self.current_price:
            self.is_losing_money = float(self.current_price) < float(self.nett_cost)

        # Below minimum
        if self.current_price and self.minimum_price:
            self.is_below_minimum = float(self.current_price) < float(self.minimum_price)

        # Above RRP
        if self.current_price and self.rrp:
            self.is_above_rrp = float(self.current_price) > float(self.rrp)

        # No cost data
        self.has_no_cost = self.nett_cost is None

    def get_margin(self) -> float:
        """Calculate profit margin percentage"""
        if not self.current_price or not self.nett_cost:
            return 0.0
        if float(self.current_price) == 0:
            return 0.0
        return ((float(self.current_price) - float(self.nett_cost)) / float(self.current_price)) * 100
