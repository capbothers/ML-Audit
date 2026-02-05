"""
Marketing campaign models (Google Ads, Klaviyo)
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, Text
from sqlalchemy.sql import func
from app.models.base import Base


class GoogleAdsCampaign(Base):
    """Google Ads campaign model"""
    __tablename__ = "google_ads_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    name = Column(String)
    status = Column(String, index=True)

    # Campaign settings
    channel_type = Column(String)
    bidding_strategy = Column(String)

    # Performance metrics
    cost = Column(Float)
    impressions = Column(Integer)
    clicks = Column(Integer)
    conversions = Column(Float)
    conversion_value = Column(Float)
    avg_cpc = Column(Float)
    ctr = Column(Float)

    # Calculated metrics
    roas = Column(Float, nullable=True)  # Return on ad spend
    cpa = Column(Float, nullable=True)  # Cost per acquisition

    # Anomaly detection
    is_anomaly = Column(Boolean, default=False)
    anomaly_score = Column(Float, nullable=True)
    anomaly_reason = Column(Text, nullable=True)

    # Timestamps
    date = Column(DateTime, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class GoogleAd(Base):
    """Individual Google Ad model"""
    __tablename__ = "google_ads"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    name = Column(String)

    # Hierarchy
    campaign_name = Column(String, index=True)
    ad_group_name = Column(String, index=True)

    # Status and approval
    status = Column(String, index=True)
    approval_status = Column(String, index=True)
    review_status = Column(String)
    ad_type = Column(String)

    # Policy violations
    is_disapproved = Column(Boolean, default=False, index=True)
    violations = Column(JSON)  # List of policy violations

    # Alert tracking
    alert_sent = Column(Boolean, default=False)
    alert_sent_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class KlaviyoCampaign(Base):
    """Klaviyo email campaign model"""
    __tablename__ = "klaviyo_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    name = Column(String)
    subject = Column(String)
    status = Column(String, index=True)

    # Send info
    send_time = Column(DateTime, index=True)

    # Engagement metrics
    sent = Column(Integer, default=0)
    delivered = Column(Integer, default=0)
    opens = Column(Integer, default=0)
    unique_opens = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    unique_clicks = Column(Integer, default=0)
    bounces = Column(Integer, default=0)
    unsubscribes = Column(Integer, default=0)
    spam_complaints = Column(Integer, default=0)

    # Calculated rates
    open_rate = Column(Float)
    click_rate = Column(Float)
    bounce_rate = Column(Float)

    # Performance flags
    low_performance = Column(Boolean, default=False)
    high_unsubscribe_rate = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
