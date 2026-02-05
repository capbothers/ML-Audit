"""
Klaviyo Data Models

Stores campaign, flow, and segment data from Klaviyo.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class KlaviyoCampaign(Base):
    """Klaviyo campaign performance"""
    __tablename__ = "klaviyo_campaigns"

    id = Column(Integer, primary_key=True, index=True)

    # Campaign identification
    campaign_id = Column(String, unique=True, index=True, nullable=False)
    campaign_name = Column(String, nullable=False)

    # Campaign details
    status = Column(String, index=True, nullable=True)
    # Status: sent, scheduled, draft, canceled

    subject_line = Column(String, nullable=True)
    from_name = Column(String, nullable=True)
    from_email = Column(String, nullable=True)

    # Timing
    send_time = Column(DateTime, nullable=True, index=True)
    created_at_klaviyo = Column(DateTime, nullable=True)

    # Recipients
    recipients = Column(Integer, default=0)

    # Engagement metrics
    opens = Column(Integer, default=0)
    unique_opens = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    unique_clicks = Column(Integer, default=0)
    bounces = Column(Integer, default=0)
    spam_complaints = Column(Integer, default=0)
    unsubscribes = Column(Integer, default=0)

    # Rates
    open_rate = Column(Float, nullable=True)
    click_rate = Column(Float, nullable=True)
    bounce_rate = Column(Float, nullable=True)
    unsubscribe_rate = Column(Float, nullable=True)

    # Revenue metrics
    conversions = Column(Integer, default=0)
    revenue = Column(Numeric(10, 2), default=0)
    conversion_rate = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<KlaviyoCampaign {self.campaign_name}>"


class KlaviyoFlow(Base):
    """Klaviyo flow (automated email sequence) performance"""
    __tablename__ = "klaviyo_flows"

    id = Column(Integer, primary_key=True, index=True)

    # Flow identification
    flow_id = Column(String, unique=True, index=True, nullable=False)
    flow_name = Column(String, nullable=False)

    # Flow details
    status = Column(String, index=True, nullable=True)
    # Status: live, draft, manual

    trigger_type = Column(String, nullable=True)
    # e.g., Added to List, Placed Order, Abandoned Cart, etc.

    # Timing
    created_at_klaviyo = Column(DateTime, nullable=True)
    updated_at_klaviyo = Column(DateTime, nullable=True)

    # Performance metrics (aggregate across all emails in flow)
    recipients = Column(Integer, default=0)
    opens = Column(Integer, default=0)
    unique_opens = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    unique_clicks = Column(Integer, default=0)
    unsubscribes = Column(Integer, default=0)

    # Rates
    open_rate = Column(Float, nullable=True)
    click_rate = Column(Float, nullable=True)

    # Revenue metrics
    conversions = Column(Integer, default=0)
    revenue = Column(Numeric(10, 2), default=0)
    conversion_rate = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<KlaviyoFlow {self.flow_name}>"


class KlaviyoFlowMessage(Base):
    """Individual messages within a Klaviyo flow"""
    __tablename__ = "klaviyo_flow_messages"

    id = Column(Integer, primary_key=True, index=True)

    # Message identification
    message_id = Column(String, unique=True, index=True, nullable=False)
    flow_id = Column(String, index=True, nullable=False)
    message_name = Column(String, nullable=True)

    # Message details
    subject_line = Column(String, nullable=True)
    delay_minutes = Column(Integer, nullable=True)
    # Delay after trigger or previous message

    # Performance metrics
    recipients = Column(Integer, default=0)
    opens = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    conversions = Column(Integer, default=0)
    revenue = Column(Numeric(10, 2), default=0)

    # Rates
    open_rate = Column(Float, nullable=True)
    click_rate = Column(Float, nullable=True)
    conversion_rate = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<KlaviyoFlowMessage {self.message_name}>"


class KlaviyoSegment(Base):
    """Klaviyo list/segment"""
    __tablename__ = "klaviyo_segments"

    id = Column(Integer, primary_key=True, index=True)

    # Segment identification
    segment_id = Column(String, unique=True, index=True, nullable=False)
    segment_name = Column(String, nullable=False)

    # Segment type
    segment_type = Column(String, nullable=True)
    # Types: list, segment

    # Size
    member_count = Column(Integer, default=0, index=True)

    # Growth
    member_count_change_30d = Column(Integer, default=0)
    # Change in member count over last 30 days

    # Metadata
    created_at_klaviyo = Column(DateTime, nullable=True)
    updated_at_klaviyo = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<KlaviyoSegment {self.segment_name} ({self.member_count} members)>"


class KlaviyoProfile(Base):
    """Klaviyo profile (customer) engagement summary"""
    __tablename__ = "klaviyo_profiles"

    id = Column(Integer, primary_key=True, index=True)

    # Profile identification
    profile_id = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, index=True, nullable=False)

    # Profile details
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    phone_number = Column(String, nullable=True)

    # Engagement
    created_at_klaviyo = Column(DateTime, nullable=True)
    last_event_date = Column(DateTime, nullable=True, index=True)
    last_open_date = Column(DateTime, nullable=True)
    last_click_date = Column(DateTime, nullable=True)

    # Counts
    total_opens = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    total_orders = Column(Integer, default=0)

    # Revenue
    total_revenue = Column(Numeric(10, 2), default=0)
    average_order_value = Column(Numeric(10, 2), nullable=True)

    # Segments
    segments = Column(JSON, nullable=True)
    # List of segment IDs this profile belongs to

    # Status
    is_subscribed = Column(Boolean, default=True)
    unsubscribed_at = Column(DateTime, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<KlaviyoProfile {self.email}>"
