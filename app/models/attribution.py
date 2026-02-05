"""
Attribution Models

Tracks customer journey touchpoints across channels to provide
honest multi-touch attribution (not Google's last-click version)

Answers: "Where should I actually spend my next dollar?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class CustomerTouchpoint(Base):
    """
    Individual touchpoint in a customer journey

    Tracks every interaction: ad click, email open, organic visit, etc.
    """
    __tablename__ = "customer_touchpoints"

    id = Column(Integer, primary_key=True, index=True)

    # Customer identification
    customer_id = Column(Integer, ForeignKey("customers.id"), index=True, nullable=True)
    journey_id = Column(Integer, ForeignKey("customer_journeys.id"), index=True, nullable=True)
    session_id = Column(String, index=True)  # GA4 session ID
    user_id = Column(String, index=True)  # GA4 user ID or email hash

    # Touchpoint details
    timestamp = Column(DateTime, index=True)
    touchpoint_type = Column(String, index=True)  # click, impression, visit, email_open, email_click

    # Channel/source attribution
    channel = Column(String, index=True)  # google_ads, meta_ads, email, organic, direct
    source = Column(String)  # google, facebook, klaviyo, (direct)
    medium = Column(String)  # cpc, email, organic, (none)
    campaign = Column(String, nullable=True)

    # UTM parameters
    utm_source = Column(String, nullable=True)
    utm_medium = Column(String, nullable=True)
    utm_campaign = Column(String, nullable=True)
    utm_content = Column(String, nullable=True)
    utm_term = Column(String, nullable=True)

    # Ad-specific data
    ad_id = Column(String, nullable=True)
    ad_group_id = Column(String, nullable=True)
    keyword = Column(String, nullable=True)

    # Email-specific data
    email_campaign_id = Column(String, nullable=True)
    email_flow_id = Column(String, nullable=True)

    # Page/content
    landing_page = Column(String, nullable=True)
    referrer = Column(String, nullable=True)

    # Device/context
    device_category = Column(String, nullable=True)  # desktop, mobile, tablet

    # Attribution value
    attributed_revenue = Column(Float, default=0.0)  # From multi-touch model
    attribution_percentage = Column(Float, default=0.0)  # % of credit for conversion

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    journey = relationship("CustomerJourney", back_populates="touchpoints")


class CustomerJourney(Base):
    """
    Complete customer journey from first touch to conversion

    Links all touchpoints for a customer into a cohesive journey
    """
    __tablename__ = "customer_journeys"

    id = Column(Integer, primary_key=True, index=True)

    # Customer identification
    customer_id = Column(Integer, ForeignKey("customers.id"), index=True, nullable=True)
    user_id = Column(String, unique=True, index=True)  # GA4 user ID or email hash

    # Journey timeline
    first_touch_date = Column(DateTime, index=True)
    last_touch_date = Column(DateTime)
    conversion_date = Column(DateTime, nullable=True, index=True)

    # Journey metrics
    touchpoint_count = Column(Integer, default=0)
    days_to_conversion = Column(Integer, nullable=True)

    # First touch attribution
    first_touch_channel = Column(String, index=True)
    first_touch_source = Column(String)
    first_touch_campaign = Column(String, nullable=True)

    # Last touch attribution (Google's version)
    last_touch_channel = Column(String, index=True)
    last_touch_source = Column(String)
    last_touch_campaign = Column(String, nullable=True)

    # Multi-touch attribution (the truth)
    assisted_channels = Column(JSON)  # All channels that assisted
    channel_touchpoints = Column(JSON)  # Count by channel: {"google_ads": 3, "email": 2, "organic": 1}

    # Conversion
    converted = Column(Boolean, default=False, index=True)
    order_id = Column(String, nullable=True)
    revenue = Column(Float, default=0.0)

    # Attribution models results
    linear_attribution = Column(JSON, nullable=True)  # Equal credit to all touchpoints
    time_decay_attribution = Column(JSON, nullable=True)  # More credit to recent touchpoints
    position_based_attribution = Column(JSON, nullable=True)  # 40% first, 40% last, 20% middle
    data_driven_attribution = Column(JSON, nullable=True)  # ML-based attribution

    # Journey path (simplified)
    journey_path = Column(Text, nullable=True)  # e.g., "google_ads -> email -> organic -> email -> direct"

    # Customer value
    customer_ltv = Column(Float, nullable=True)
    is_first_purchase = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    touchpoints = relationship("CustomerTouchpoint", back_populates="journey")


class ChannelAttribution(Base):
    """
    Aggregated attribution by channel

    Compares different attribution models to show which channels
    are over/under-credited by last-click (Google's default)
    """
    __tablename__ = "channel_attribution"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    period_start = Column(DateTime, index=True)
    period_end = Column(DateTime, index=True)
    period_type = Column(String)  # daily, weekly, monthly

    # Channel
    channel = Column(String, index=True)  # google_ads, meta_ads, email, organic, direct

    # Last-click attribution (Google's version)
    last_click_conversions = Column(Integer, default=0)
    last_click_revenue = Column(Float, default=0.0)
    last_click_credit_pct = Column(Float, default=0.0)

    # First-click attribution
    first_click_conversions = Column(Integer, default=0)
    first_click_revenue = Column(Float, default=0.0)
    first_click_credit_pct = Column(Float, default=0.0)

    # Multi-touch attribution (linear - equal credit)
    linear_conversions = Column(Float, default=0.0)  # Fractional
    linear_revenue = Column(Float, default=0.0)
    linear_credit_pct = Column(Float, default=0.0)

    # Multi-touch attribution (time decay)
    time_decay_conversions = Column(Float, default=0.0)
    time_decay_revenue = Column(Float, default=0.0)
    time_decay_credit_pct = Column(Float, default=0.0)

    # Multi-touch attribution (position-based: 40-20-40)
    position_conversions = Column(Float, default=0.0)
    position_revenue = Column(Float, default=0.0)
    position_credit_pct = Column(Float, default=0.0)

    # Assisted conversions
    assisted_conversions = Column(Integer, default=0)  # How many conversions this channel assisted
    assist_ratio = Column(Float, default=0.0)  # Assisted / Last-click

    # The truth: over/under credit analysis
    credit_difference_pct = Column(Float, default=0.0)  # Multi-touch vs Last-click
    is_overcredited = Column(Boolean, default=False)  # Last-click gives too much credit
    is_undercredited = Column(Boolean, default=False)  # Last-click gives too little credit

    # Spend data (if available)
    total_spend = Column(Float, default=0.0)

    # Efficiency metrics
    true_roas = Column(Float, nullable=True)  # Multi-touch revenue / spend
    reported_roas = Column(Float, nullable=True)  # Last-click revenue / spend
    true_cac = Column(Float, nullable=True)  # Multi-touch
    reported_cac = Column(Float, nullable=True)  # Last-click

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AttributionInsight(Base):
    """
    LLM-generated attribution insights and recommendations

    Stores the "so what" analysis of attribution data
    """
    __tablename__ = "attribution_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    period_start = Column(DateTime, index=True)
    period_end = Column(DateTime)

    # Insight type
    insight_type = Column(String, index=True)  # overcredited_channel, undercredited_channel, budget_reallocation

    # Channels involved
    channel = Column(String, nullable=True)
    related_channels = Column(JSON, nullable=True)

    # Key findings
    title = Column(String)
    description = Column(Text)
    severity = Column(String)  # critical, high, medium, low

    # Impact quantification
    estimated_impact_dollars = Column(Float, nullable=True)
    estimated_impact_conversions = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)  # 0-1

    # Recommendations
    recommended_action = Column(Text)
    expected_outcome = Column(Text, nullable=True)

    # LLM analysis
    llm_analysis = Column(Text, nullable=True)

    # Status
    is_active = Column(Boolean, default=True)
    acknowledged = Column(Boolean, default=False)
    acknowledged_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
