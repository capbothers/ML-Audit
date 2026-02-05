"""
Product profitability models
Track true product-level profitability after all costs
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class Product(Base):
    """Product master data with cost and margin info"""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    shopify_product_id = Column(String, unique=True, index=True)

    # Basic info
    title = Column(String, index=True)
    sku = Column(String, index=True)
    variant_id = Column(String)
    variant_title = Column(String)

    # Pricing
    price = Column(Float)  # Current selling price
    cost = Column(Float)  # Cost of goods sold (COGS)
    compare_at_price = Column(Float, nullable=True)

    # Margin data
    base_margin_pct = Column(Float)  # (price - cost) / price
    base_margin_dollars = Column(Float)  # price - cost

    # Product attributes
    category = Column(String, nullable=True)
    collection = Column(String, nullable=True)
    tags = Column(JSON, nullable=True)

    # Inventory
    inventory_quantity = Column(Integer, default=0)
    inventory_value = Column(Float, default=0.0)  # quantity * cost

    # Status
    is_active = Column(Boolean, default=True)
    published_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    sales = relationship("ProductSale", back_populates="product")
    profitability_snapshots = relationship("ProductProfitability", back_populates="product")


class ProductSale(Base):
    """Individual product sales with attribution"""
    __tablename__ = "product_sales"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), index=True)

    # Order info
    order_id = Column(String, index=True)
    order_date = Column(DateTime, index=True)

    # Sale details
    quantity = Column(Integer)
    unit_price = Column(Float)
    total_revenue = Column(Float)  # quantity * unit_price

    # Costs
    unit_cost = Column(Float)  # COGS per unit at time of sale
    total_cost = Column(Float)  # quantity * unit_cost

    # Gross margin (before marketing spend)
    gross_margin = Column(Float)  # total_revenue - total_cost
    gross_margin_pct = Column(Float)

    # Attribution (if available)
    traffic_source = Column(String, nullable=True)  # google, facebook, organic, direct, email
    campaign_name = Column(String, nullable=True)
    utm_source = Column(String, nullable=True)
    utm_medium = Column(String, nullable=True)
    utm_campaign = Column(String, nullable=True)

    # Attributed ad spend (if from paid channel)
    attributed_ad_spend = Column(Float, default=0.0)

    # Return status
    was_returned = Column(Boolean, default=False)
    return_date = Column(DateTime, nullable=True)
    refund_amount = Column(Float, default=0.0)

    # Net margin (after ad spend and returns)
    net_margin = Column(Float)  # gross_margin - attributed_ad_spend - refund_amount
    net_margin_pct = Column(Float)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    product = relationship("Product", back_populates="sales")


class ProductProfitability(Base):
    """
    Product profitability snapshots
    Aggregated view of product performance over time periods
    """
    __tablename__ = "product_profitability"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), index=True)

    # Time period
    period_start = Column(DateTime, index=True)
    period_end = Column(DateTime, index=True)
    period_type = Column(String)  # daily, weekly, monthly

    # Volume metrics
    units_sold = Column(Integer, default=0)
    orders_count = Column(Integer, default=0)

    # Revenue
    total_revenue = Column(Float, default=0.0)
    average_unit_price = Column(Float, default=0.0)

    # Costs
    total_cogs = Column(Float, default=0.0)  # Cost of goods sold
    gross_margin_dollars = Column(Float, default=0.0)  # revenue - cogs
    gross_margin_pct = Column(Float, default=0.0)

    # Marketing attribution
    attributed_ad_spend = Column(Float, default=0.0)  # Total ad spend driving these sales
    ad_spend_by_channel = Column(JSON, nullable=True)  # Breakdown by channel

    # Returns
    units_returned = Column(Integer, default=0)
    return_rate_pct = Column(Float, default=0.0)
    total_refunded = Column(Float, default=0.0)

    # Net profitability (the truth)
    net_revenue = Column(Float, default=0.0)  # After returns
    net_profit_dollars = Column(Float, default=0.0)  # After COGS, ad spend, returns
    net_profit_margin_pct = Column(Float, default=0.0)
    roas = Column(Float, nullable=True)  # Return on ad spend

    # Efficiency metrics
    cost_per_acquisition = Column(Float, nullable=True)  # ad_spend / orders_count
    customer_acquisition_cost = Column(Float, nullable=True)  # For first-time buyers

    # Traffic and conversion
    product_page_views = Column(Integer, default=0)
    conversion_rate = Column(Float, default=0.0)

    # Ranking and classification
    profit_rank = Column(Integer, nullable=True)  # Ranking by net profit
    profitability_tier = Column(String, nullable=True)  # gold, silver, bronze, losing_money

    # Flags
    is_profitable = Column(Boolean, default=True)
    is_breakeven = Column(Boolean, default=False)
    is_losing_money = Column(Boolean, default=False)

    # Insights
    performance_vs_baseline = Column(Float, nullable=True)  # % vs average product
    opportunity_score = Column(Float, nullable=True)  # 0-100 score for growth potential

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    product = relationship("Product", back_populates="profitability_snapshots")


class ProductAdSpendAllocation(Base):
    """
    Ad spend allocation to products
    Tracks how much ad spend is attributed to each product
    """
    __tablename__ = "product_ad_spend_allocation"

    id = Column(Integer, primary_key=True, index=True)

    # Product
    product_id = Column(Integer, ForeignKey("products.id"), index=True)
    shopify_product_id = Column(String, index=True)

    # Campaign
    campaign_id = Column(String, index=True)
    campaign_name = Column(String)
    channel = Column(String)  # google_ads, meta_ads, etc.

    # Date
    date = Column(DateTime, index=True)

    # Spend
    allocated_spend = Column(Float)  # Portion of campaign spend allocated to this product
    allocation_method = Column(String)  # click_through, view_through, last_click, etc.

    # Performance
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    conversions = Column(Integer, default=0)
    revenue_attributed = Column(Float, default=0.0)

    # Efficiency
    cpc = Column(Float, nullable=True)  # Cost per click
    cpa = Column(Float, nullable=True)  # Cost per acquisition
    roas = Column(Float, nullable=True)  # Return on ad spend

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
