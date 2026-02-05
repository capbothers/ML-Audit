"""
User Behavior Intelligence Models

Analyzes user behavior patterns from Hotjar/Clarity to identify friction points.
Answers: "Where are users getting stuck? What's frustrating them?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, Numeric
from datetime import datetime
from decimal import Decimal

from app.models.base import Base


class PageFriction(Base):
    """
    Page-level friction metrics

    Identifies pages with high traffic but low conversion, rage clicks, dead clicks
    """
    __tablename__ = "page_friction"

    id = Column(Integer, primary_key=True, index=True)

    # Page identification
    page_path = Column(String, unique=True, index=True, nullable=False)
    page_title = Column(String, nullable=True)
    page_type = Column(String, index=True)  # product, category, checkout, home

    # Traffic metrics
    total_sessions = Column(Integer, default=0)
    unique_visitors = Column(Integer, default=0)
    page_views = Column(Integer, default=0, index=True)

    # Engagement metrics
    avg_time_on_page = Column(Float, nullable=True)  # Seconds
    median_scroll_depth = Column(Float, nullable=True)  # 0-100%
    bounce_rate = Column(Float, nullable=True)  # 0-1
    exit_rate = Column(Float, nullable=True)  # 0-1

    # Conversion metrics
    conversion_rate = Column(Float, default=0.0, index=True)  # % who convert
    avg_conversion_rate = Column(Float, nullable=True)  # Site average
    conversion_rate_gap = Column(Float, nullable=True)  # % points below average

    # Friction indicators
    rage_click_count = Column(Integer, default=0, index=True)  # Rapid repeated clicks
    rage_click_sessions = Column(Integer, default=0)  # Sessions with rage clicks
    rage_click_rate = Column(Float, nullable=True)  # % sessions with rage clicks

    dead_click_count = Column(Integer, default=0, index=True)  # Clicks on non-clickable
    dead_click_sessions = Column(Integer, default=0)
    dead_click_rate = Column(Float, nullable=True)

    error_click_count = Column(Integer, default=0)  # Clicks that trigger errors

    # Friction hotspots
    friction_elements = Column(JSON, nullable=True)  # [{element, click_count, issue}, ...]
    """
    Example:
    [
        {"element": ".size-guide-icon", "click_count": 47, "issue": "not_clickable"},
        {"element": "#shipping-calculator", "click_count": 89, "issue": "appears_broken"}
    ]
    """

    # Scroll depth issues
    content_below_fold = Column(Boolean, default=False)  # Critical content not visible
    percent_reach_cta = Column(Float, nullable=True)  # % who scroll to CTA
    percent_reach_specs = Column(Float, nullable=True)  # % who scroll to product specs

    # Mobile vs Desktop
    mobile_conversion_rate = Column(Float, nullable=True)
    desktop_conversion_rate = Column(Float, nullable=True)
    mobile_desktop_gap = Column(Float, nullable=True)  # % points difference

    # Problem severity
    is_high_friction = Column(Boolean, default=False, index=True)
    friction_score = Column(Integer, default=0, index=True)  # 0-100 (100 = severe friction)
    severity = Column(String, index=True)  # critical, high, medium, low

    # Revenue impact
    estimated_monthly_traffic = Column(Integer, default=0)
    estimated_monthly_revenue_lost = Column(Numeric(10, 2), default=0, index=True)

    # Recommendations
    issues_detected = Column(JSON, nullable=True)  # List of specific issues
    recommended_fixes = Column(JSON, nullable=True)  # [{fix, impact, effort}, ...]
    priority = Column(String, index=True)  # critical, high, medium, low

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    analyzed_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CheckoutFunnel(Base):
    """
    Checkout funnel step-by-step analysis

    Identifies where customers drop off in checkout process
    """
    __tablename__ = "checkout_funnel"

    id = Column(Integer, primary_key=True, index=True)

    # Funnel identification
    funnel_name = Column(String, default="Standard Checkout")

    # Step definitions
    step_number = Column(Integer, index=True)  # 1, 2, 3, etc.
    step_name = Column(String, nullable=False)  # Cart, Checkout, Shipping, Payment, Confirm
    step_url = Column(String, nullable=True)

    # Step metrics
    sessions_entered = Column(Integer, default=0)
    sessions_completed = Column(Integer, default=0)
    sessions_dropped = Column(Integer, default=0)

    completion_rate = Column(Float, nullable=True, index=True)  # % who complete this step
    drop_off_rate = Column(Float, nullable=True, index=True)  # % who abandon here

    # Time metrics
    avg_time_on_step = Column(Float, nullable=True)  # Seconds
    median_time_on_step = Column(Float, nullable=True)

    # Previous step (for comparison)
    previous_step_completion_rate = Column(Float, nullable=True)
    step_to_step_drop_rate = Column(Float, nullable=True)  # Drop from prev step to this

    # Friction indicators
    rage_click_count = Column(Integer, default=0)
    error_message_count = Column(Integer, default=0)
    form_field_errors = Column(JSON, nullable=True)  # Which fields cause errors

    # Common issues at this step
    stuck_sessions = Column(Integer, default=0)  # Sessions that stay here > 5min
    back_button_clicks = Column(Integer, default=0)  # Users going back
    reload_count = Column(Integer, default=0)  # Page reloads (confusion?)

    # Mobile vs Desktop
    mobile_completion_rate = Column(Float, nullable=True)
    desktop_completion_rate = Column(Float, nullable=True)
    mobile_desktop_gap = Column(Float, nullable=True)

    # Revenue impact
    estimated_sessions_per_month = Column(Integer, default=0)
    estimated_revenue_lost = Column(Numeric(10, 2), default=0, index=True)
    avg_order_value = Column(Numeric(10, 2), nullable=True)

    # Problem analysis
    is_biggest_leak = Column(Boolean, default=False, index=True)
    issues_detected = Column(JSON, nullable=True)
    recommended_fixes = Column(JSON, nullable=True)
    priority = Column(String, index=True)

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    analyzed_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DeviceComparison(Base):
    """
    Mobile vs Desktop performance comparison

    Identifies pages with mobile-specific UX problems
    """
    __tablename__ = "device_comparison"

    id = Column(Integer, primary_key=True, index=True)

    # Page identification
    page_path = Column(String, index=True, nullable=False)
    page_type = Column(String, index=True)

    # Desktop metrics
    desktop_sessions = Column(Integer, default=0)
    desktop_conversion_rate = Column(Float, nullable=True)
    desktop_avg_time = Column(Float, nullable=True)
    desktop_bounce_rate = Column(Float, nullable=True)

    # Mobile metrics
    mobile_sessions = Column(Integer, default=0)
    mobile_conversion_rate = Column(Float, nullable=True)
    mobile_avg_time = Column(Float, nullable=True)
    mobile_bounce_rate = Column(Float, nullable=True)

    # Tablet metrics (optional)
    tablet_sessions = Column(Integer, default=0)
    tablet_conversion_rate = Column(Float, nullable=True)

    # Performance gaps
    conversion_rate_gap = Column(Float, nullable=True, index=True)  # % points
    mobile_underperforming = Column(Boolean, default=False, index=True)

    # Mobile-specific issues
    mobile_rage_clicks = Column(Integer, default=0)
    mobile_dead_clicks = Column(Integer, default=0)
    mobile_scroll_issues = Column(Boolean, default=False)

    mobile_specific_problems = Column(JSON, nullable=True)
    """
    Example:
    [
        {"issue": "cta_below_fold", "affected_sessions": 234},
        {"issue": "touch_target_too_small", "element": ".filter-apply"},
        {"issue": "horizontal_scroll", "viewport_width": 375}
    ]
    """

    # Layout issues
    content_cut_off_mobile = Column(Boolean, default=False)
    horizontal_scroll_mobile = Column(Boolean, default=False)
    small_touch_targets = Column(JSON, nullable=True)  # Elements with touch issues

    # Revenue impact
    mobile_traffic_percentage = Column(Float, nullable=True)
    estimated_mobile_revenue_lost = Column(Numeric(10, 2), default=0, index=True)

    # Problem severity
    severity = Column(String, index=True)
    priority = Column(String, index=True)

    # Recommendations
    recommended_fixes = Column(JSON, nullable=True)

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    analyzed_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SessionInsight(Base):
    """
    Session-level behavior patterns

    Common frustration patterns and user behavior insights
    """
    __tablename__ = "session_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Pattern identification
    pattern_name = Column(String, unique=True, index=True, nullable=False)
    pattern_type = Column(String, index=True)  # frustration, abandonment, success

    # Pattern characteristics
    description = Column(Text, nullable=True)
    common_pages = Column(JSON, nullable=True)  # Pages where this occurs
    common_devices = Column(String, nullable=True)  # mobile, desktop, both

    # Prevalence
    sessions_with_pattern = Column(Integer, default=0)
    percentage_of_total = Column(Float, nullable=True)

    # Typical sequence
    event_sequence = Column(JSON, nullable=True)
    """
    Example:
    [
        {"event": "page_view", "page": "/product/abc"},
        {"event": "rage_click", "element": ".size-guide"},
        {"event": "exit"}
    ]
    """

    # Outcomes
    conversion_rate = Column(Float, nullable=True)
    avg_session_value = Column(Numeric(10, 2), nullable=True)
    bounce_rate = Column(Float, nullable=True)

    # Time correlation
    avg_time_before_pattern = Column(Float, nullable=True)  # Seconds before frustration
    time_to_conversion_if_overcome = Column(Float, nullable=True)

    # Revenue impact
    estimated_sessions_per_month = Column(Integer, default=0)
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)

    # Recommendations
    is_fixable = Column(Boolean, default=True)
    recommended_actions = Column(JSON, nullable=True)
    priority = Column(String, index=True)

    # Analysis period
    period_start = Column(DateTime, nullable=True)
    period_end = Column(DateTime, nullable=True)
    period_days = Column(Integer, default=30)

    # Timestamps
    identified_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UserBehaviorInsight(Base):
    """
    LLM-generated user behavior insights

    Strategic analysis of friction points and UX improvements
    """
    __tablename__ = "user_behavior_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Insight type
    insight_type = Column(String, index=True)  # page_friction, checkout_funnel, mobile_issues, session_pattern, overall

    # Analysis
    title = Column(String, nullable=False)
    analysis = Column(Text, nullable=False)  # LLM-generated analysis
    key_findings = Column(JSON, nullable=True)  # [{finding, impact}, ...]

    # Specific page/element
    page_path = Column(String, nullable=True, index=True)
    problem_element = Column(String, nullable=True)

    # Problem diagnosis
    issue_description = Column(Text, nullable=True)
    root_cause = Column(Text, nullable=True)
    user_frustration_signals = Column(JSON, nullable=True)

    # Recommendations
    recommended_fixes = Column(JSON, nullable=True)  # [{fix, impact, effort, implementation}, ...]
    priority = Column(String, index=True)  # critical, high, medium, low

    # Impact
    estimated_revenue_impact = Column(Numeric(10, 2), default=0)
    affected_sessions_per_month = Column(Integer, nullable=True)
    conversion_lift_potential = Column(Float, nullable=True)  # % points

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
