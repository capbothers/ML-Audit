"""
Business Expense & Monthly P&L Models

Tracks operating expenses (payroll, rent, shipping, etc.) and generates
monthly P&L statements that feed overhead allocation into campaign profitability.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, Numeric, Date
from datetime import datetime

from app.models.base import Base


EXPENSE_CATEGORIES = [
    "payroll",
    "rent",
    "shipping",
    "utilities",
    "insurance",
    "software",
    "marketing_other",  # Non-Google Ads marketing
    "professional_services",  # Accounting, legal
    "other",
]


class BusinessExpense(Base):
    """Individual business expense entry (imported via CSV)"""
    __tablename__ = "business_expenses"

    id = Column(Integer, primary_key=True, index=True)

    # Period
    month = Column(Date, index=True, nullable=False)  # First of month (2026-01-01)

    # Classification
    category = Column(String, index=True, nullable=False)
    # Categories: payroll, rent, shipping, utilities, insurance, software,
    #             marketing_other, professional_services, other

    description = Column(String, nullable=False)  # "Staff wages", "Warehouse rent"
    amount = Column(Numeric(12, 2), nullable=False)  # AUD inc GST

    # Metadata
    is_recurring = Column(Boolean, default=True)
    notes = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<BusinessExpense {self.category}: {self.description} ${self.amount} ({self.month})>"


class MonthlyPL(Base):
    """
    Monthly Profit & Loss statement

    Aggregated from Shopify revenue, product costs, Google Ads spend,
    and business expenses. Recalculated on demand.
    """
    __tablename__ = "monthly_pl"

    id = Column(Integer, primary_key=True, index=True)
    month = Column(Date, unique=True, index=True, nullable=False)  # First of month

    # Revenue
    gross_revenue = Column(Numeric(12, 2), default=0)   # Shopify total_price sum
    refunds = Column(Numeric(12, 2), default=0)         # Total refunded amount
    net_revenue = Column(Numeric(12, 2), default=0)     # gross_revenue - refunds

    # Cost of Goods Sold
    cogs = Column(Numeric(12, 2), default=0)            # From ProductCost via ShopifyOrderItem

    # Gross Margin
    gross_margin = Column(Numeric(12, 2), default=0)    # net_revenue - cogs
    gross_margin_pct = Column(Numeric(5, 2), nullable=True)

    # Operating Expenses
    ad_spend = Column(Numeric(12, 2), default=0)        # Google Ads cost_micros sum
    payroll = Column(Numeric(12, 2), default=0)
    rent = Column(Numeric(12, 2), default=0)
    shipping = Column(Numeric(12, 2), default=0)
    utilities = Column(Numeric(12, 2), default=0)
    insurance = Column(Numeric(12, 2), default=0)
    software = Column(Numeric(12, 2), default=0)
    marketing_other = Column(Numeric(12, 2), default=0)
    professional_services = Column(Numeric(12, 2), default=0)
    other_expenses = Column(Numeric(12, 2), default=0)
    total_expenses = Column(Numeric(12, 2), default=0)  # Sum of all expense categories

    # Operating Profit
    operating_profit = Column(Numeric(12, 2), default=0)  # gross_margin - total_expenses
    operating_margin_pct = Column(Numeric(5, 2), nullable=True)

    # Net Profit (same as operating for now — no interest/tax modelling)
    net_profit = Column(Numeric(12, 2), default=0)
    net_margin_pct = Column(Numeric(5, 2), nullable=True)

    # Order metrics
    total_orders = Column(Integer, default=0)
    avg_order_value = Column(Numeric(10, 2), nullable=True)

    # Overhead allocation
    overhead_per_order = Column(Numeric(10, 2), nullable=True)
    # = (total_expenses excl ad_spend excl cogs) / total_orders
    # Used by AdSpendProcessor to calculate fully-loaded campaign profit

    # Timestamps
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<MonthlyPL {self.month}: revenue=${self.net_revenue} profit=${self.net_profit}>"
