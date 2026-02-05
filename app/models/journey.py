"""
Customer Journey Intelligence Models

Analyzes customer behavior patterns, LTV segmentation, and product impact.
Answers: "What separates high-LTV customers from one-and-done buyers?"
"""
from sqlalchemy import Column, Integer, BigInteger, String, Float, DateTime, JSON, Boolean, Text, Date, Numeric
from datetime import datetime, date
from decimal import Decimal

from app.models.base import Base


class CustomerLTV(Base):
    """
    Customer lifetime value analysis and segmentation

    Calculated from Shopify customer data
    """
    __tablename__ = "customer_ltv"

    id = Column(Integer, primary_key=True, index=True)

    # Customer identification
    customer_id = Column(Integer, index=True, nullable=False)  # Internal customer ID
    shopify_customer_id = Column(BigInteger, index=True, nullable=True)
    email = Column(String, index=True, nullable=True)

    # LTV metrics
    total_revenue = Column(Numeric(10, 2), default=0, index=True)
    total_orders = Column(Integer, default=0, index=True)
    avg_order_value = Column(Numeric(10, 2), default=0)

    # Calculated LTV (can include predicted future value)
    historical_ltv = Column(Numeric(10, 2), default=0)  # Actual spend so far
    predicted_ltv = Column(Numeric(10, 2), nullable=True)  # ML prediction of future value
    total_ltv = Column(Numeric(10, 2), default=0, index=True)  # Historical + predicted

    # LTV segment (dynamically calculated)
    ltv_segment = Column(String, index=True)  # top_20, middle_60, bottom_20
    ltv_percentile = Column(Float, nullable=True)  # 0-100

    # Journey characteristics
    first_order_date = Column(Date, index=True)
    last_order_date = Column(Date, index=True)
    days_as_customer = Column(Integer, default=0)
    days_since_last_order = Column(Integer, nullable=True, index=True)

    # First purchase details
    first_product_id = Column(Integer, nullable=True)
    first_product_title = Column(String, nullable=True)
    first_product_sku = Column(String, nullable=True)
    first_order_channel = Column(String, nullable=True)  # utm_source
    first_order_value = Column(Numeric(10, 2), nullable=True)

    # Journey timing
    days_to_second_order = Column(Integer, nullable=True, index=True)  # KEY METRIC
    avg_days_between_orders = Column(Float, nullable=True)

    # Purchase patterns
    purchase_frequency_days = Column(Float, nullable=True)  # Avg days between purchases
    is_repeat_customer = Column(Boolean, default=False, index=True)
    repeat_purchase_rate = Column(Float, nullable=True)  # % of customers who returned

    # Engagement indicators
    email_subscriber = Column(Boolean, default=False)
    subscribed_before_first_purchase = Column(Boolean, default=False)
    avg_email_engagement = Column(Float, nullable=True)  # Open/click rate

    # Product affinity
    favorite_product_category = Column(String, nullable=True)
    product_variety_score = Column(Float, nullable=True)  # How many different products bought

    # Churn risk
    is_at_risk = Column(Boolean, default=False, index=True)
    churn_probability = Column(Float, nullable=True)  # 0-1
    expected_next_purchase_date = Column(Date, nullable=True)

    # Gateway/dead-end product indicator
    bought_gateway_product = Column(Boolean, default=False)
    bought_dead_end_product = Column(Boolean, default=False)

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class JourneyPattern(Base):
    """
    Common journey patterns identified in customer data

    Patterns that lead to high vs low LTV
    """
    __tablename__ = "journey_patterns"

    id = Column(Integer, primary_key=True, index=True)

    # Pattern identification
    pattern_name = Column(String, unique=True, index=True, nullable=False)
    pattern_type = Column(String, index=True)  # high_ltv, low_ltv, churn_risk

    # Pattern characteristics
    first_product_category = Column(String, nullable=True)
    first_channel = Column(String, nullable=True)  # utm_source
    avg_days_to_second_purchase = Column(Float, nullable=True)
    email_subscribed_first = Column(Boolean, nullable=True)

    # Prevalence
    customer_count = Column(Integer, default=0)
    percentage_of_segment = Column(Float, nullable=True)  # % of high/low LTV customers

    # Outcomes
    avg_ltv = Column(Numeric(10, 2), default=0)
    avg_orders = Column(Float, nullable=True)
    avg_aov = Column(Numeric(10, 2), nullable=True)
    repeat_purchase_rate = Column(Float, nullable=True)

    # Comparison to baseline
    ltv_vs_baseline = Column(Float, nullable=True)  # % difference from avg
    repeat_rate_vs_baseline = Column(Float, nullable=True)

    # Pattern description
    description = Column(Text, nullable=True)
    key_characteristics = Column(JSON, nullable=True)  # [{characteristic, value}, ...]

    # Recommendations
    is_desirable_pattern = Column(Boolean, default=False)
    recommended_actions = Column(JSON, nullable=True)  # How to encourage this pattern

    # Timestamps
    identified_at = Column(DateTime, default=datetime.utcnow, index=True)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class GatewayProduct(Base):
    """
    Products that act as gateways to repeat purchases

    First purchase of these products → high repeat rate → high LTV
    """
    __tablename__ = "gateway_products"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(Integer, index=True, nullable=False)
    shopify_product_id = Column(BigInteger, index=True, nullable=True)
    product_title = Column(String, nullable=False)
    product_sku = Column(String, index=True, nullable=True)
    product_category = Column(String, index=True, nullable=True)

    # Gateway metrics
    total_first_purchases = Column(Integer, default=0)  # Customers who bought this first
    repeat_customers = Column(Integer, default=0)  # How many came back
    repeat_purchase_rate = Column(Float, default=0.0, index=True)  # % who returned

    # Compared to average
    avg_repeat_rate_all_products = Column(Float, nullable=True)
    repeat_rate_lift = Column(Float, nullable=True)  # X times higher than average

    # LTV impact
    avg_ltv_from_this_product = Column(Numeric(10, 2), default=0)
    avg_ltv_all_customers = Column(Numeric(10, 2), nullable=True)
    ltv_multiplier = Column(Float, nullable=True, index=True)  # X times higher LTV

    # Journey timing
    avg_days_to_second_purchase = Column(Float, nullable=True)
    avg_total_orders = Column(Float, nullable=True)

    # Current promotion status
    is_featured = Column(Boolean, default=False)
    is_in_ads = Column(Boolean, default=False)
    is_in_email_flows = Column(Boolean, default=False)
    current_promotion_score = Column(Integer, default=0)  # 0-100

    # Opportunity
    should_be_promoted = Column(Boolean, default=False, index=True)
    promotion_opportunity_score = Column(Integer, default=0, index=True)  # 0-100
    estimated_ltv_gain = Column(Numeric(10, 2), default=0)  # If featured more prominently

    # Recommendations
    recommended_actions = Column(JSON, nullable=True)  # Where to promote this

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DeadEndProduct(Base):
    """
    Products that correlate with customer churn

    First purchase of these products → low repeat rate → one-and-done
    """
    __tablename__ = "dead_end_products"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(Integer, index=True, nullable=False)
    shopify_product_id = Column(BigInteger, index=True, nullable=True)
    product_title = Column(String, nullable=False)
    product_sku = Column(String, index=True, nullable=True)
    product_category = Column(String, index=True, nullable=True)

    # Dead-end metrics
    total_first_purchases = Column(Integer, default=0)
    one_time_customers = Column(Integer, default=0)  # Never came back
    one_time_rate = Column(Float, default=0.0, index=True)  # % who never returned

    # Compared to average
    avg_one_time_rate_all_products = Column(Float, nullable=True)
    one_time_rate_difference = Column(Float, nullable=True)  # % points higher than average

    # Why customers don't return
    return_rate = Column(Float, nullable=True)  # Product return rate
    avg_customer_rating = Column(Float, nullable=True)
    negative_review_rate = Column(Float, nullable=True)

    # LTV impact
    avg_ltv_from_this_product = Column(Numeric(10, 2), default=0)
    avg_ltv_all_customers = Column(Numeric(10, 2), nullable=True)
    ltv_penalty = Column(Float, nullable=True)  # How much lower than average

    # Customer type attracted
    avg_discount_used = Column(Float, nullable=True)  # % discount on first purchase
    price_sensitivity_score = Column(Float, nullable=True)  # Attracts bargain hunters?

    # Current promotion status
    is_featured = Column(Boolean, default=False)
    is_in_ads = Column(Boolean, default=False)
    current_ad_spend = Column(Numeric(10, 2), nullable=True)
    promotion_score = Column(Integer, default=0)  # 0-100

    # Problem severity
    severity = Column(String, index=True)  # critical, high, medium, low
    is_actively_promoted = Column(Boolean, default=False)  # Getting traffic but killing LTV

    # Estimated damage
    estimated_ltv_lost = Column(Numeric(10, 2), default=0)  # From promoting this

    # Recommendations
    recommended_actions = Column(JSON, nullable=True)  # What to do about this product
    should_stop_promoting = Column(Boolean, default=False, index=True)

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ChurnRiskTiming(Base):
    """
    Optimal timing for customer reactivation

    When do customers typically churn? When should we reach out?
    """
    __tablename__ = "churn_risk_timing"

    id = Column(Integer, primary_key=True, index=True)

    # Segment
    ltv_segment = Column(String, index=True)  # top_20, middle_60, bottom_20
    product_category = Column(String, index=True, nullable=True)  # Can vary by category

    # Timing metrics
    avg_days_between_purchases = Column(Float, nullable=False)
    median_days_between_purchases = Column(Float, nullable=True)
    std_dev_days = Column(Float, nullable=True)

    # Risk thresholds
    at_risk_threshold_days = Column(Integer, nullable=False, index=True)  # Days since last order = at risk
    critical_risk_threshold_days = Column(Integer, nullable=True)  # Likely churned

    # Reactivation windows
    optimal_reactivation_day_min = Column(Integer, nullable=True)  # Start reaching out
    optimal_reactivation_day_max = Column(Integer, nullable=True)  # Latest effective time
    reactivation_success_rate = Column(Float, nullable=True)  # % who respond in this window

    # Current at-risk customers
    customers_at_risk = Column(Integer, default=0)
    customers_critical_risk = Column(Integer, default=0)
    total_ltv_at_risk = Column(Numeric(10, 2), default=0)

    # Win-back effectiveness
    winback_open_rate = Column(Float, nullable=True)
    winback_conversion_rate = Column(Float, nullable=True)
    avg_winback_order_value = Column(Numeric(10, 2), nullable=True)

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CustomerJourneyInsight(Base):
    """
    LLM-generated customer journey insights

    Strategic analysis of what makes high-LTV customers different
    """
    __tablename__ = "customer_journey_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Insight type
    insight_type = Column(String, index=True)  # ltv_patterns, gateway_product, dead_end_product, churn_timing, overall

    # Analysis
    title = Column(String, nullable=False)
    analysis = Column(Text, nullable=False)  # LLM-generated analysis
    key_findings = Column(JSON, nullable=True)  # [{finding, impact}, ...]

    # Recommendations
    recommended_actions = Column(JSON, nullable=True)  # [{action, impact, effort}, ...]
    priority = Column(String, index=True)  # critical, high, medium, low

    # Impact
    estimated_ltv_improvement = Column(Numeric(10, 2), default=0)
    estimated_repeat_rate_improvement = Column(Float, nullable=True)  # % points
    affected_customers = Column(Integer, nullable=True)

    # Effort
    effort_level = Column(String)  # low, medium, high
    implementation_time = Column(String, nullable=True)

    # Supporting data
    supporting_data = Column(JSON, nullable=True)

    # Status
    status = Column(String, default='active', index=True)  # active, implemented, dismissed
    implemented_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
