"""
Weekly Strategic Brief Models

Synthesizes insights from all modules into a prioritized action list.
Answers: "What should I focus on this week?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, Numeric, Date
from datetime import datetime, date
from decimal import Decimal

from app.models.base import Base


class WeeklyBrief(Base):
    """
    Weekly strategic brief

    Synthesizes all module insights into prioritized actions
    """
    __tablename__ = "weekly_briefs"

    id = Column(Integer, primary_key=True, index=True)

    # Brief identification
    week_start_date = Column(Date, unique=True, index=True, nullable=False)
    week_end_date = Column(Date, nullable=False)
    week_number = Column(Integer, index=True)  # ISO week number
    year = Column(Integer, index=True)

    # Data quality
    data_quality_score = Column(Integer, default=0)  # 0-100
    data_quality_status = Column(String)  # excellent, good, fair, poor
    data_issues = Column(JSON, nullable=True)  # List of data quality issues

    # Priority summary
    total_priorities = Column(Integer, default=0)
    high_priority_count = Column(Integer, default=0)
    medium_priority_count = Column(Integer, default=0)
    low_priority_count = Column(Integer, default=0)

    # Impact summary
    total_revenue_opportunity = Column(Numeric(10, 2), default=0, index=True)
    total_cost_savings = Column(Numeric(10, 2), default=0)
    total_estimated_impact = Column(Numeric(10, 2), default=0)  # Revenue + Savings

    # Module contribution
    module_insights_count = Column(JSON, nullable=True)
    """
    {
        "ad_spend": 3,
        "email": 2,
        "journey": 1,
        "seo": 2,
        "behavior": 1
    }
    """

    # Top priorities (preview)
    top_3_priorities = Column(JSON, nullable=True)  # Quick reference

    # What's working
    working_well_items = Column(JSON, nullable=True)
    working_well_count = Column(Integer, default=0)

    # Watch list
    watch_list_items = Column(JSON, nullable=True)
    watch_list_count = Column(Integer, default=0)

    # Trends vs previous week
    trends_summary = Column(JSON, nullable=True)
    """
    {
        "improved": ["ROAS up 12%", "Email opens up 5%"],
        "declined": ["Mobile conversion down 3%"],
        "implemented": ["SEO title fix"],
        "pending": ["Abandoned cart sequence"]
    }
    """

    # Executive summary (LLM-generated)
    executive_summary = Column(Text, nullable=True)
    key_takeaways = Column(JSON, nullable=True)  # 3-5 bullet points

    # Status
    is_current = Column(Boolean, default=True, index=True)
    was_sent = Column(Boolean, default=False)
    sent_at = Column(DateTime, nullable=True)

    # Timestamps
    generated_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BriefPriority(Base):
    """
    Individual priority items in weekly brief

    Scored and ranked priorities from all modules
    """
    __tablename__ = "brief_priorities"

    id = Column(Integer, primary_key=True, index=True)

    # Brief relationship
    brief_id = Column(Integer, index=True, nullable=False)
    week_start_date = Column(Date, index=True, nullable=False)

    # Priority identification
    priority_rank = Column(Integer, index=True)  # 1, 2, 3, etc.
    priority_title = Column(String, nullable=False)
    priority_description = Column(Text, nullable=True)

    # Source
    source_module = Column(String, index=True, nullable=False)  # ad_spend, email, journey, etc.
    source_insight_id = Column(Integer, nullable=True)  # Reference to original insight
    source_insight_type = Column(String, nullable=True)

    # Impact
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)
    estimated_cost_savings = Column(Numeric(10, 2), default=0)
    total_estimated_impact = Column(Numeric(10, 2), default=0, index=True)
    impact_timeframe = Column(String)  # weekly, monthly, quarterly

    # Effort
    effort_level = Column(String, index=True)  # low, medium, high
    effort_hours = Column(Float, nullable=True)
    effort_description = Column(String, nullable=True)

    # Confidence
    confidence_level = Column(String)  # high, medium, low
    confidence_score = Column(Float, nullable=True)  # 0-1
    data_supporting = Column(Text, nullable=True)

    # Priority score
    priority_score = Column(Float, default=0, index=True)  # Impact × Confidence / Effort
    """
    Formula: (revenue_impact × confidence_score) / effort_hours
    Higher score = higher priority
    """

    # Action items
    recommended_action = Column(Text, nullable=False)
    action_steps = Column(JSON, nullable=True)  # Step-by-step implementation
    responsible_team = Column(String, nullable=True)  # dev, marketing, ops

    # Dependencies
    requires = Column(JSON, nullable=True)  # Prerequisites
    blocks = Column(JSON, nullable=True)  # What this blocks

    # Priority level
    priority_level = Column(String, index=True)  # critical, high, medium, low

    # Status tracking
    status = Column(String, default='new', index=True)  # new, in_progress, completed, deferred
    completed_at = Column(DateTime, nullable=True)
    deferred_reason = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BriefTrend(Base):
    """
    Week-over-week trend comparisons

    Tracks what improved, what declined, what was implemented
    """
    __tablename__ = "brief_trends"

    id = Column(Integer, primary_key=True, index=True)

    # Brief relationship
    current_brief_id = Column(Integer, index=True, nullable=False)
    previous_brief_id = Column(Integer, nullable=True)
    current_week_start = Column(Date, index=True, nullable=False)

    # Trend type
    trend_type = Column(String, index=True, nullable=False)  # improved, declined, implemented, pending, new

    # Metric being tracked
    metric_name = Column(String, nullable=False)
    metric_category = Column(String, index=True)  # revenue, conversion, traffic, engagement

    # Values
    previous_value = Column(Float, nullable=True)
    current_value = Column(Float, nullable=True)
    change_absolute = Column(Float, nullable=True)
    change_percentage = Column(Float, nullable=True)

    # Description
    description = Column(Text, nullable=False)
    significance = Column(String)  # major, moderate, minor

    # Context
    source_module = Column(String, nullable=True)
    related_priority_id = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class BriefWorkingWell(Base):
    """
    Things that are working well

    Don't touch these - they're performing above benchmarks
    """
    __tablename__ = "brief_working_well"

    id = Column(Integer, primary_key=True, index=True)

    # Brief relationship
    brief_id = Column(Integer, index=True, nullable=False)
    week_start_date = Column(Date, index=True, nullable=False)

    # Item identification
    item_name = Column(String, nullable=False)
    item_type = Column(String, index=True)  # campaign, flow, page, product, channel

    # Source
    source_module = Column(String, index=True, nullable=False)

    # Performance
    metric_name = Column(String, nullable=False)
    current_value = Column(Float, nullable=False)
    benchmark_value = Column(Float, nullable=True)
    performance_vs_benchmark = Column(Float, nullable=True)  # % above benchmark

    # Why it's working
    success_factors = Column(JSON, nullable=True)
    description = Column(Text, nullable=False)

    # Keep monitoring
    stability = Column(String)  # stable, improving, volatile
    weeks_performing_well = Column(Integer, default=1)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class BriefWatchList(Base):
    """
    Emerging issues to monitor

    Not urgent yet, but worth watching
    """
    __tablename__ = "brief_watch_list"

    id = Column(Integer, primary_key=True, index=True)

    # Brief relationship
    brief_id = Column(Integer, index=True, nullable=False)
    week_start_date = Column(Date, index=True, nullable=False)

    # Issue identification
    issue_name = Column(String, nullable=False)
    issue_type = Column(String, index=True)  # performance_decline, new_competitor, technical_issue

    # Source
    source_module = Column(String, index=True, nullable=False)

    # Severity
    severity = Column(String, index=True)  # monitor, investigate, urgent
    urgency_score = Column(Integer, default=0)  # 0-100

    # Description
    description = Column(Text, nullable=False)
    observed_since = Column(Date, nullable=True)
    weeks_on_watch_list = Column(Integer, default=1)

    # Thresholds
    current_value = Column(Float, nullable=True)
    alert_threshold = Column(Float, nullable=True)
    action_threshold = Column(Float, nullable=True)  # When to escalate to priority

    # Trend
    trend_direction = Column(String)  # improving, stable, worsening

    # If escalated
    escalated_to_priority = Column(Boolean, default=False, index=True)
    escalated_at = Column(DateTime, nullable=True)
    escalated_priority_id = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BriefInsight(Base):
    """
    LLM-generated insights for weekly brief

    Executive-level analysis and recommendations
    """
    __tablename__ = "brief_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Brief relationship
    brief_id = Column(Integer, index=True, nullable=False)
    week_start_date = Column(Date, index=True, nullable=False)

    # Insight type
    insight_type = Column(String, index=True)  # executive_summary, priority_analysis, trend_analysis

    # Analysis
    title = Column(String, nullable=False)
    analysis = Column(Text, nullable=False)  # LLM-generated analysis

    # Key points
    key_takeaways = Column(JSON, nullable=True)  # Bullet points
    """
    [
        "Focus on checkout shipping calc fix - highest ROI",
        "Ad budget reallocation is low-effort, high-impact",
        "Mobile performance declining - monitor closely"
    ]
    """

    # Recommendations
    strategic_recommendations = Column(JSON, nullable=True)
    """
    [
        {
            "recommendation": "Prioritize UX fixes this week",
            "rationale": "3 high-impact, low-effort opportunities",
            "expected_impact": "$8,200/month"
        }
    ]
    """

    # Context
    data_quality_note = Column(Text, nullable=True)
    confidence_note = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
