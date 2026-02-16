"""
Ad Spend Optimization Intelligence Models

Analyzes Google Ads performance with true ROAS calculations.
Answers: "Where am I wasting ad spend? Where should I scale?"
"""
from sqlalchemy import Column, Integer, BigInteger, String, Float, DateTime, JSON, Boolean, Text, Numeric
from datetime import datetime
from decimal import Decimal

from app.models.base import Base


class CampaignPerformance(Base):
    """
    Google Ads campaign performance with true ROAS

    Uses actual product costs and Shopify revenue (not Google's conversion value)
    """
    __tablename__ = "campaign_performance"

    id = Column(Integer, primary_key=True, index=True)

    # Campaign identification
    campaign_id = Column(String, unique=True, index=True, nullable=False)
    campaign_name = Column(String, nullable=False)
    campaign_type = Column(String, index=True)  # search, shopping, pmax, display, video

    # Status
    is_active = Column(Boolean, default=True, index=True)
    budget_status = Column(String, nullable=True)  # limited, standard, capped

    # Spend metrics
    total_spend = Column(Numeric(10, 2), default=0, index=True)
    daily_budget = Column(Numeric(10, 2), nullable=True)
    avg_daily_spend = Column(Numeric(10, 2), nullable=True)

    # Budget pacing
    budget_capped = Column(Boolean, default=False, index=True)  # Runs out before end of day
    avg_cap_time = Column(String, nullable=True)  # e.g., "2:00 PM" - when budget runs out
    lost_impression_share = Column(Float, nullable=True)  # % of impressions lost to budget

    # Click metrics
    total_clicks = Column(Integer, default=0)
    avg_cpc = Column(Numeric(10, 2), nullable=True)
    click_through_rate = Column(Float, nullable=True)  # 0-1

    # Impression metrics
    total_impressions = Column(Integer, default=0)
    avg_position = Column(Float, nullable=True)

    # Conversion metrics (from Google)
    google_conversions = Column(Integer, default=0)
    google_conversion_value = Column(Numeric(10, 2), default=0)  # What Google reports
    google_roas = Column(Float, nullable=True, index=True)  # Google's ROAS

    # True metrics (from Shopify)
    actual_conversions = Column(Integer, default=0)  # Matched to Shopify orders
    actual_revenue = Column(Numeric(10, 2), default=0)  # Real Shopify revenue
    actual_product_costs = Column(Numeric(10, 2), default=0)  # From Google Sheets

    # True ROAS calculation
    true_profit = Column(Numeric(10, 2), default=0, index=True)  # Revenue - Costs - Spend
    true_roas = Column(Float, nullable=True, index=True)  # Profit / Spend
    revenue_roas = Column(Float, nullable=True)  # Revenue / Spend (without costs)

    # Performance indicators
    is_profitable = Column(Boolean, default=False, index=True)
    is_high_performer = Column(Boolean, default=False, index=True)  # ROAS > threshold
    is_scaling_opportunity = Column(Boolean, default=False, index=True)  # High ROAS + budget capped

    # Waste detection
    is_wasting_budget = Column(Boolean, default=False, index=True)
    waste_reasons = Column(JSON, nullable=True)  # List of waste reasons
    estimated_waste = Column(Numeric(10, 2), default=0)  # $ wasted per month

    # Product mix
    products_advertised = Column(Integer, default=0)
    avg_product_margin = Column(Float, nullable=True)
    unprofitable_products_count = Column(Integer, default=0)  # Products below margin threshold

    # Fully-loaded profitability (includes allocated operating overhead)
    allocated_overhead = Column(Numeric(12, 2), nullable=True)       # overhead_per_order * orders
    fully_loaded_profit = Column(Numeric(12, 2), nullable=True)      # true_profit - allocated_overhead
    fully_loaded_roas = Column(Float, nullable=True)                 # (revenue - cogs - overhead) / spend
    is_profitable_fully_loaded = Column(Boolean, nullable=True)      # fully_loaded_profit > 0

    # Recommendations
    recommended_action = Column(String, index=True)  # scale, reduce, pause, optimize
    recommended_budget = Column(Numeric(10, 2), nullable=True)
    expected_impact = Column(Numeric(10, 2), nullable=True)  # Expected profit change

    # Strategy-aware decision layer
    strategy_type = Column(String, nullable=True, index=True)       # high_consideration, fast_turn, brand_defense, prospecting, unknown
    decision_score = Column(Integer, nullable=True)                  # 0-100 composite
    short_term_status = Column(String, nullable=True)                # strong, healthy, marginal, weak
    strategic_value = Column(String, nullable=True)                  # high, moderate, low
    strategy_action = Column(String, nullable=True, index=True)      # scale_aggressively, scale, maintain, optimize, reduce, pause, investigate, fix_landing_page
    strategy_confidence = Column(String, nullable=True)              # high, medium, low

    # Causal triage (Capability 1)
    primary_cause = Column(String, nullable=True)                    # demand, auction_pressure, landing_page, attribution_lag, catalog_feed, measurement
    cause_confidence = Column(Float, nullable=True)                  # 0-1
    cause_evidence = Column(JSON, nullable=True)                     # [{cause, score, evidence}]

    # Attribution confidence (Capability 2)
    attribution_confidence = Column(String, nullable=True)           # high, medium, low
    attribution_gap_pct = Column(Float, nullable=True)               # % gap: Google vs Shopify conversions

    # Landing page friction (Capability 3)
    lp_cvr_change = Column(Float, nullable=True)                    # Period-over-period CVR change %
    lp_bounce_change = Column(Float, nullable=True)                 # Period-over-period bounce change %
    lp_is_friction = Column(Boolean, nullable=True)                 # True if LP is primary issue

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    analyzed_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AdSpendOptimization(Base):
    """
    Budget reallocation recommendations

    Models what happens if you shift spend between campaigns
    """
    __tablename__ = "ad_spend_optimizations"

    id = Column(Integer, primary_key=True, index=True)

    # Optimization identification
    optimization_name = Column(String, nullable=False)
    optimization_type = Column(String, index=True)  # reallocation, increase, decrease, pause

    # Source campaign (reduce budget from)
    source_campaign_id = Column(String, index=True, nullable=True)
    source_campaign_name = Column(String, nullable=True)
    current_source_budget = Column(Numeric(10, 2), nullable=True)
    recommended_source_budget = Column(Numeric(10, 2), nullable=True)
    budget_to_move = Column(Numeric(10, 2), nullable=True)

    # Target campaign (increase budget to)
    target_campaign_id = Column(String, index=True, nullable=True)
    target_campaign_name = Column(String, nullable=True)
    current_target_budget = Column(Numeric(10, 2), nullable=True)
    recommended_target_budget = Column(Numeric(10, 2), nullable=True)
    budget_to_add = Column(Numeric(10, 2), nullable=True)

    # Current performance
    current_total_spend = Column(Numeric(10, 2), default=0)
    current_total_revenue = Column(Numeric(10, 2), default=0)
    current_total_profit = Column(Numeric(10, 2), default=0)

    # Projected performance
    projected_total_spend = Column(Numeric(10, 2), default=0)
    projected_total_revenue = Column(Numeric(10, 2), default=0)
    projected_total_profit = Column(Numeric(10, 2), default=0)

    # Expected impact
    revenue_impact = Column(Numeric(10, 2), default=0)  # Additional revenue
    profit_impact = Column(Numeric(10, 2), default=0, index=True)  # Additional profit
    spend_change = Column(Numeric(10, 2), default=0)  # Change in total spend

    # Confidence
    confidence_level = Column(String)  # high, medium, low
    confidence_score = Column(Float, nullable=True)  # 0-1

    # Reasoning
    rationale = Column(Text, nullable=True)
    supporting_data = Column(JSON, nullable=True)

    # Priority
    priority = Column(String, index=True)  # critical, high, medium, low
    impact_score = Column(Integer, default=0, index=True)  # 0-100

    # Status
    status = Column(String, default='recommended', index=True)  # recommended, implemented, dismissed
    implemented_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AdWaste(Base):
    """
    Identified ad spend waste

    Specific instances where money is being wasted
    """
    __tablename__ = "ad_waste"

    id = Column(Integer, primary_key=True, index=True)

    # Waste identification
    waste_type = Column(String, index=True, nullable=False)
    """
    Types:
    - brand_cannibalization: Brand campaigns capturing organic traffic
    - below_margin_products: Advertising products with margins too low
    - no_conversion_keywords: High spend keywords with zero conversions
    - duplicate_targeting: Multiple campaigns targeting same audience
    - budget_fragmentation: Too many small campaigns
    - poor_quality_score: High CPC due to low quality score
    """

    waste_description = Column(Text, nullable=False)

    # Associated campaign/product
    campaign_id = Column(String, index=True, nullable=True)
    campaign_name = Column(String, nullable=True)
    product_id = Column(Integer, nullable=True)
    product_title = Column(String, nullable=True)
    keyword = Column(String, nullable=True)

    # Waste metrics
    monthly_waste_spend = Column(Numeric(10, 2), default=0, index=True)  # $ wasted per month
    organic_conversion_rate = Column(Float, nullable=True)  # For brand cannibalization
    actual_conversion_rate = Column(Float, nullable=True)

    # Product-specific (for below_margin_products)
    product_margin = Column(Float, nullable=True)  # Actual margin
    margin_threshold = Column(Float, nullable=True)  # Minimum margin needed
    cost_per_acquisition = Column(Numeric(10, 2), nullable=True)

    # Supporting evidence
    evidence = Column(JSON, nullable=True)
    """
    Example for brand_cannibalization:
    {
        "organic_traffic": 847,
        "paid_traffic": 423,
        "estimated_organic_conversions": 310,
        "paid_conversions": 340
    }
    """

    # Severity
    severity = Column(String, index=True)  # critical, high, medium, low
    monthly_impact = Column(Numeric(10, 2), default=0)  # Monthly profit impact

    # Recommendations
    recommended_action = Column(Text, nullable=False)
    expected_savings = Column(Numeric(10, 2), default=0)  # Monthly savings if fixed
    implementation_difficulty = Column(String)  # easy, medium, hard

    # Status
    status = Column(String, default='active', index=True)  # active, fixed, dismissed
    fixed_at = Column(DateTime, nullable=True)

    # Analysis period
    period_days = Column(Integer, default=30)

    # Timestamps
    identified_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProductAdPerformance(Base):
    """
    Product-level ad performance

    Which products are profitable to advertise?
    """
    __tablename__ = "product_ad_performance"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(Integer, index=True, nullable=False)
    shopify_product_id = Column(BigInteger, index=True, nullable=True)
    product_title = Column(String, nullable=False)
    product_sku = Column(String, index=True, nullable=True)

    # Campaigns advertising this product
    campaign_ids = Column(JSON, nullable=True)  # List of campaign IDs
    total_campaigns = Column(Integer, default=0)

    # Ad spend
    total_ad_spend = Column(Numeric(10, 2), default=0, index=True)
    avg_cpc = Column(Numeric(10, 2), nullable=True)

    # Ad-driven performance
    ad_clicks = Column(Integer, default=0)
    ad_conversions = Column(Integer, default=0)
    ad_conversion_rate = Column(Float, nullable=True)

    ad_revenue = Column(Numeric(10, 2), default=0)  # Revenue from ad-driven sales
    ad_units_sold = Column(Integer, default=0)

    # Costs
    product_cost = Column(Numeric(10, 2), nullable=True)  # Cost per unit
    total_product_costs = Column(Numeric(10, 2), default=0)  # Cost of all units sold

    # Profitability
    gross_profit = Column(Numeric(10, 2), default=0)  # Revenue - Product Costs
    net_profit = Column(Numeric(10, 2), default=0, index=True)  # Revenue - Product Costs - Ad Spend
    profit_margin = Column(Float, nullable=True)  # Net Profit / Revenue

    # ROAS
    revenue_roas = Column(Float, nullable=True)  # Revenue / Ad Spend
    profit_roas = Column(Float, nullable=True, index=True)  # Net Profit / Ad Spend

    # Comparison to organic
    organic_conversion_rate = Column(Float, nullable=True)
    organic_units_sold = Column(Integer, default=0)

    # Performance indicators
    is_profitable_to_advertise = Column(Boolean, default=False, index=True)
    is_high_performer = Column(Boolean, default=False, index=True)
    is_losing_money = Column(Boolean, default=False, index=True)

    # Recommendations
    recommended_action = Column(String, index=True)  # scale, continue, reduce, exclude
    margin_threshold_met = Column(Boolean, default=False)
    min_margin_needed = Column(Float, nullable=True)

    recommended_max_cpc = Column(Numeric(10, 2), nullable=True)  # Max CPC to stay profitable

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    analyzed_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AdSpendInsight(Base):
    """
    LLM-generated ad spend insights

    Strategic analysis of where to scale, where to cut
    """
    __tablename__ = "ad_spend_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Insight type
    insight_type = Column(String, index=True)  # waste, scaling, reallocation, product_exclusion, overall

    # Analysis
    title = Column(String, nullable=False)
    analysis = Column(Text, nullable=False)  # LLM-generated analysis
    key_findings = Column(JSON, nullable=True)  # [{finding, impact}, ...]

    # Specific campaign/product
    campaign_id = Column(String, nullable=True, index=True)
    campaign_name = Column(String, nullable=True)
    product_id = Column(Integer, nullable=True)

    # Problem diagnosis
    issue_description = Column(Text, nullable=True)
    root_cause = Column(Text, nullable=True)
    waste_amount = Column(Numeric(10, 2), nullable=True)  # Monthly waste

    # Recommendations
    recommended_actions = Column(JSON, nullable=True)  # [{action, impact, implementation}, ...]
    priority = Column(String, index=True)  # critical, high, medium, low

    # Impact
    estimated_profit_impact = Column(Numeric(10, 2), default=0)  # Monthly profit impact
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)
    spend_change = Column(Numeric(10, 2), default=0)  # Change in spend

    # Effort
    effort_level = Column(String)  # low, medium, high
    implementation_time = Column(String, nullable=True)

    # Supporting data
    supporting_metrics = Column(JSON, nullable=True)

    # Status
    status = Column(String, default='active', index=True)  # active, implemented, dismissed
    implemented_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
