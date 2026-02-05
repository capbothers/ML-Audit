"""
Hotjar / Microsoft Clarity Data Models

Stores heatmap data, funnel analysis, and session recording insights.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class HotjarPageData(Base):
    """Page heatmap and behavior data from Hotjar/Clarity"""
    __tablename__ = "hotjar_pages"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Page
    page_url = Column(String, index=True, nullable=False)
    page_title = Column(String, nullable=True)

    # Traffic
    total_sessions = Column(Integer, default=0)
    total_pageviews = Column(Integer, default=0)

    # Scroll depth
    scroll_depth_avg = Column(Float, nullable=True)
    # Average % of page scrolled (0-100)
    scroll_to_bottom_rate = Column(Float, nullable=True)
    # % of sessions that scrolled to bottom

    # Click behavior
    click_count = Column(Integer, default=0)
    rage_click_count = Column(Integer, default=0)
    # Rapid repeated clicks (frustration indicator)
    dead_click_count = Column(Integer, default=0)
    # Clicks that didn't do anything

    # Engagement metrics
    avg_time_on_page = Column(Float, nullable=True)
    # In seconds
    quick_back_rate = Column(Float, nullable=True)
    # % who left within 5 seconds

    # Device breakdown
    desktop_sessions = Column(Integer, default=0)
    mobile_sessions = Column(Integer, default=0)
    tablet_sessions = Column(Integer, default=0)

    # Frustration indicators
    error_click_count = Column(Integer, default=0)
    # Clicks on error messages
    excessive_scrolling_count = Column(Integer, default=0)
    # Back-and-forth scrolling (confusion)

    # Heat zones (stored as JSON)
    click_heatmap_data = Column(JSON, nullable=True)
    # Top clicked elements with coordinates
    scroll_heatmap_data = Column(JSON, nullable=True)
    # Scroll depth distribution

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<HotjarPage {self.page_url} - {self.date}>"


class HotjarFunnelStep(Base):
    """Funnel step data from Hotjar/Clarity"""
    __tablename__ = "hotjar_funnel_steps"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Funnel
    funnel_name = Column(String, index=True, nullable=False)
    # e.g., "Checkout Funnel", "Registration Funnel"

    # Step
    step_number = Column(Integer, nullable=False)
    step_name = Column(String, nullable=False)
    step_url = Column(String, nullable=True)

    # Metrics
    sessions_entered = Column(Integer, default=0)
    sessions_completed = Column(Integer, default=0)
    sessions_dropped = Column(Integer, default=0)

    # Rates
    completion_rate = Column(Float, nullable=True)
    drop_off_rate = Column(Float, nullable=True)

    # Time metrics
    avg_time_on_step = Column(Float, nullable=True)
    # In seconds

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<HotjarFunnelStep {self.funnel_name} - Step {self.step_number}>"


class HotjarRecordingSummary(Base):
    """Session recording insights from Hotjar/Clarity"""
    __tablename__ = "hotjar_recordings"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Page
    page_url = Column(String, index=True, nullable=False)

    # Device
    device_type = Column(String, index=True, nullable=True)
    # Types: desktop, mobile, tablet

    # Recording metrics (aggregated)
    total_recordings = Column(Integer, default=0)
    recordings_with_rage_clicks = Column(Integer, default=0)
    recordings_with_u_turns = Column(Integer, default=0)
    # User goes back/forward repeatedly
    recordings_with_errors = Column(Integer, default=0)

    # Engagement
    avg_time_on_page = Column(Float, nullable=True)
    avg_scroll_depth = Column(Float, nullable=True)

    # Frustration score
    frustration_score = Column(Float, nullable=True)
    # Calculated score 0-100 based on frustration signals

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<HotjarRecording {self.page_url} - {self.device_type} - {self.date}>"


class HotjarPoll(Base):
    """Poll/survey results from Hotjar"""
    __tablename__ = "hotjar_polls"

    id = Column(Integer, primary_key=True, index=True)

    # Poll identification
    poll_id = Column(String, unique=True, index=True, nullable=False)
    poll_name = Column(String, nullable=False)

    # Poll details
    poll_question = Column(Text, nullable=True)
    page_url = Column(String, index=True, nullable=True)
    # Page where poll appears

    # Responses
    total_responses = Column(Integer, default=0)
    response_rate = Column(Float, nullable=True)
    # % of visitors who responded

    # Results (stored as JSON)
    response_distribution = Column(JSON, nullable=True)
    # e.g., {"Yes": 45, "No": 32, "Maybe": 23}

    # Metadata
    created_at = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<HotjarPoll {self.poll_name}>"


class ClaritySession(Base):
    """Microsoft Clarity session-level insights"""
    __tablename__ = "clarity_sessions"

    id = Column(Integer, primary_key=True, index=True)

    # Session identification
    session_id = Column(String, unique=True, index=True, nullable=False)

    # Date
    session_date = Column(DateTime, index=True, nullable=False)

    # Session details
    device_type = Column(String, nullable=True)
    country = Column(String, nullable=True)
    referrer = Column(String, nullable=True)

    # Pages viewed
    pages_viewed = Column(JSON, nullable=True)
    # List of page URLs

    # Session metrics
    session_duration = Column(Float, nullable=True)
    # In seconds
    total_clicks = Column(Integer, default=0)
    total_scrolls = Column(Integer, default=0)

    # Frustration signals
    has_rage_clicks = Column(Boolean, default=False)
    has_dead_clicks = Column(Boolean, default=False)
    has_excessive_scrolling = Column(Boolean, default=False)
    has_quick_backs = Column(Boolean, default=False)

    # Conversion
    converted = Column(Boolean, default=False)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<ClaritySession {self.session_id}>"
