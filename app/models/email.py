"""
Email & Retention Intelligence Models

Analyzes email marketing performance (Klaviyo).
Answers: "Am I emailing enough? What flows are underperforming?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, Date, Numeric
from datetime import datetime, date
from decimal import Decimal

from app.models.base import Base


class EmailCampaign(Base):
    """
    Email campaign performance data

    Synced from Klaviyo API
    """
    __tablename__ = "email_campaigns"

    id = Column(Integer, primary_key=True, index=True)

    # Klaviyo IDs
    campaign_id = Column(String, unique=True, index=True, nullable=False)
    campaign_name = Column(String, nullable=False)

    # Campaign details
    subject_line = Column(Text, nullable=True)
    preview_text = Column(Text, nullable=True)
    from_name = Column(String, nullable=True)

    # Targeting
    segment_name = Column(String, index=True, nullable=True)
    segment_id = Column(String, index=True, nullable=True)
    list_name = Column(String, nullable=True)

    # Send metrics
    total_recipients = Column(Integer, default=0)
    total_sent = Column(Integer, default=0)
    total_bounced = Column(Integer, default=0)
    bounce_rate = Column(Float, default=0.0)

    # Engagement metrics
    total_opens = Column(Integer, default=0)
    unique_opens = Column(Integer, default=0)
    open_rate = Column(Float, default=0.0)  # Unique opens / delivered

    total_clicks = Column(Integer, default=0)
    unique_clicks = Column(Integer, default=0)
    click_rate = Column(Float, default=0.0)  # Unique clicks / delivered
    click_to_open_rate = Column(Float, default=0.0)  # Clicks / opens

    # Negative metrics
    total_unsubscribes = Column(Integer, default=0)
    unsubscribe_rate = Column(Float, default=0.0)
    total_spam_complaints = Column(Integer, default=0)
    spam_rate = Column(Float, default=0.0)

    # Revenue metrics
    total_revenue = Column(Numeric(10, 2), default=0)
    attributed_orders = Column(Integer, default=0)
    revenue_per_recipient = Column(Numeric(10, 2), default=0)

    # Performance indicators
    is_high_performing = Column(Boolean, default=False, index=True)  # Top 20% by revenue
    is_underperforming = Column(Boolean, default=False, index=True)  # Below benchmarks

    # Timestamps
    sent_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    synced_at = Column(DateTime, default=datetime.utcnow)


class EmailFlow(Base):
    """
    Email flow (automation) performance with benchmarks

    Flows: Welcome, Abandoned Cart, Browse Abandonment, Post-Purchase, Win-Back, etc.
    """
    __tablename__ = "email_flows"

    id = Column(Integer, primary_key=True, index=True)

    # Klaviyo IDs
    flow_id = Column(String, unique=True, index=True, nullable=False)
    flow_name = Column(String, nullable=False)
    flow_type = Column(String, index=True)  # welcome, abandoned_cart, post_purchase, winback, browse_abandonment, etc.

    # Flow configuration
    is_active = Column(Boolean, default=True, index=True)
    total_emails = Column(Integer, default=0)  # Number of emails in flow
    trigger_type = Column(String, nullable=True)  # What triggers this flow

    # Period performance (last 30 days by default)
    period_days = Column(Integer, default=30)
    period_start = Column(Date, index=True)
    period_end = Column(Date)

    # Flow metrics
    total_entered = Column(Integer, default=0)  # People who entered flow
    total_sent = Column(Integer, default=0)  # Total emails sent
    total_bounced = Column(Integer, default=0)

    # Engagement
    unique_opens = Column(Integer, default=0)
    open_rate = Column(Float, default=0.0)
    unique_clicks = Column(Integer, default=0)
    click_rate = Column(Float, default=0.0)
    click_to_open_rate = Column(Float, default=0.0)

    # Conversions
    total_conversions = Column(Integer, default=0)
    conversion_rate = Column(Float, default=0.0)  # Conversions / entered
    total_revenue = Column(Numeric(10, 2), default=0)
    revenue_per_recipient = Column(Numeric(10, 2), default=0)

    # Benchmarks (industry averages for this flow type)
    benchmark_open_rate = Column(Float, nullable=True)  # Expected open rate
    benchmark_click_rate = Column(Float, nullable=True)
    benchmark_conversion_rate = Column(Float, nullable=True)

    # Performance vs benchmark
    open_rate_vs_benchmark = Column(Float, nullable=True)  # % difference
    click_rate_vs_benchmark = Column(Float, nullable=True)
    conversion_rate_vs_benchmark = Column(Float, nullable=True)

    is_underperforming = Column(Boolean, default=False, index=True)  # Below benchmark
    performance_score = Column(Integer, default=0)  # 0-100

    # Revenue opportunity
    estimated_revenue_gap = Column(Numeric(10, 2), default=0)  # Lost revenue vs benchmark

    # Issues detected
    issues_detected = Column(JSON, nullable=True)  # [{issue, impact, fix}, ...]
    recommended_actions = Column(JSON, nullable=True)  # [action1, action2, ...]

    # Timestamps
    last_calculated = Column(DateTime, default=datetime.utcnow, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow)


class EmailSegment(Base):
    """
    Email segment health and engagement

    Identifies high-value segments that aren't being contacted enough
    """
    __tablename__ = "email_segments"

    id = Column(Integer, primary_key=True, index=True)

    # Klaviyo IDs
    segment_id = Column(String, unique=True, index=True, nullable=False)
    segment_name = Column(String, nullable=False)
    segment_definition = Column(JSON, nullable=True)  # Klaviyo segment rules

    # Segment size
    total_profiles = Column(Integer, default=0, index=True)
    engaged_profiles = Column(Integer, default=0)  # Opened/clicked in last 90 days
    engagement_rate = Column(Float, default=0.0)

    # Value indicators
    avg_customer_value = Column(Numeric(10, 2), default=0)
    total_segment_value = Column(Numeric(10, 2), default=0)
    avg_orders_per_customer = Column(Float, default=0)

    # Contact frequency
    days_since_last_send = Column(Integer, nullable=True, index=True)
    sends_last_30_days = Column(Integer, default=0)
    sends_last_90_days = Column(Integer, default=0)
    avg_sends_per_week = Column(Float, default=0)

    # Engagement metrics (last 90 days)
    open_rate_90d = Column(Float, default=0.0)
    click_rate_90d = Column(Float, default=0.0)
    conversion_rate_90d = Column(Float, default=0.0)
    revenue_90d = Column(Numeric(10, 2), default=0)

    # Segment health
    is_high_value = Column(Boolean, default=False, index=True)  # High AOV or order frequency
    is_under_contacted = Column(Boolean, default=False, index=True)  # Not sent to in 30+ days
    is_engaged = Column(Boolean, default=False, index=True)  # Good open/click rates
    is_at_risk = Column(Boolean, default=False, index=True)  # Declining engagement

    # Opportunities
    estimated_response_rate = Column(Float, nullable=True)  # Expected response if contacted
    revenue_opportunity = Column(Numeric(10, 2), default=0)  # Estimated revenue if contacted

    # Recommendations
    recommended_frequency = Column(String, nullable=True)  # "weekly", "bi-weekly", "monthly"
    recommended_campaigns = Column(JSON, nullable=True)  # Suggested campaign ideas

    # Timestamps
    last_calculated = Column(DateTime, default=datetime.utcnow, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow)


class EmailSendFrequency(Base):
    """
    Overall email send frequency analysis

    Answers: Are we sending too much or too little?
    """
    __tablename__ = "email_send_frequency"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    period_start = Column(Date, index=True)
    period_end = Column(Date)
    period_days = Column(Integer, default=30)

    # Send volume
    total_campaigns_sent = Column(Integer, default=0)
    total_flow_emails_sent = Column(Integer, default=0)
    total_emails_sent = Column(Integer, default=0)

    # Frequency metrics
    avg_emails_per_subscriber_week = Column(Float, default=0.0)
    avg_emails_per_subscriber_month = Column(Float, default=0.0)

    # Engagement by frequency
    engagement_by_frequency = Column(JSON, nullable=True)  # {frequency: {open_rate, click_rate, unsub_rate}}

    # Fatigue analysis
    engagement_dropoff_threshold = Column(Float, nullable=True)  # Emails/week where engagement drops
    current_vs_optimal = Column(Float, nullable=True)  # % difference from optimal

    # Recommendations
    optimal_frequency = Column(Float, nullable=True)  # Emails/week for max engagement
    can_send_more = Column(Boolean, default=False)
    recommended_increase_pct = Column(Float, nullable=True)

    # Impact estimate
    estimated_revenue_from_frequency_change = Column(Numeric(10, 2), default=0)

    # Timestamps
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)


class EmailInsight(Base):
    """
    LLM-generated email insights and recommendations

    Strategic analysis of email program
    """
    __tablename__ = "email_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Insight type
    insight_type = Column(String, index=True)  # overall, flow, segment, frequency, campaign

    # LLM analysis
    title = Column(String, nullable=False)
    analysis = Column(Text, nullable=False)  # LLM-generated analysis
    recommended_actions = Column(JSON, nullable=True)  # [{action, impact, effort}, ...]

    # Impact
    priority = Column(String, index=True)  # critical, high, medium, low
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)
    effort_level = Column(String)  # low, medium, high
    timeframe = Column(String, nullable=True)  # "1-2 weeks", "1 month", etc.

    # Supporting data
    supporting_data = Column(JSON, nullable=True)  # Data used to generate insight

    # Status
    status = Column(String, default='active', index=True)  # active, implemented, dismissed
    implemented_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class EmailRevenueOpportunity(Base):
    """
    Revenue opportunities identified in email program

    Specific, actionable opportunities with dollar amounts
    """
    __tablename__ = "email_revenue_opportunities"

    id = Column(Integer, primary_key=True, index=True)

    # Opportunity type
    opportunity_type = Column(String, index=True)  # flow_improvement, segment_activation, frequency_optimization, campaign_idea

    # Description
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    issue = Column(Text, nullable=True)  # What's currently wrong

    # Related entities
    flow_id = Column(String, index=True, nullable=True)
    flow_name = Column(String, nullable=True)
    segment_id = Column(String, index=True, nullable=True)
    segment_name = Column(String, nullable=True)

    # Current state
    current_performance = Column(JSON, nullable=True)  # Current metrics

    # Opportunity details
    recommended_action = Column(Text, nullable=False)
    specific_steps = Column(JSON, nullable=True)  # [{step, details}, ...]

    # Impact estimate
    estimated_monthly_revenue = Column(Numeric(10, 2), default=0, index=True)
    estimated_incremental_orders = Column(Integer, default=0)
    confidence_level = Column(String)  # high, medium, low

    # Effort
    effort_level = Column(String)  # low, medium, high
    implementation_time = Column(String, nullable=True)  # "30 minutes", "2 hours", "1 week"

    # Priority
    priority = Column(String, index=True)  # critical, high, medium, low
    impact_score = Column(Integer, default=0, index=True)  # 0-100

    # Status
    status = Column(String, default='open', index=True)  # open, in_progress, completed, dismissed
    assigned_to = Column(String, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Timestamps
    identified_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
