"""
Google Analytics 4 Data Models

Stores traffic, landing pages, products, and conversion paths from GA4.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class GA4TrafficSource(Base):
    """Traffic by source/medium/campaign (daily)"""
    __tablename__ = "ga4_traffic_sources"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Source/Medium/Campaign
    session_source = Column(String, index=True, nullable=True)
    # e.g., google, facebook, direct
    session_medium = Column(String, index=True, nullable=True)
    # e.g., organic, cpc, referral, (none)
    session_campaign_name = Column(String, index=True, nullable=True)
    # Campaign name from UTM parameters

    # Traffic metrics
    sessions = Column(Integer, default=0, index=True)
    total_users = Column(Integer, default=0)
    new_users = Column(Integer, default=0)
    engaged_sessions = Column(Integer, default=0)

    # Engagement metrics
    bounce_rate = Column(Float, nullable=True)
    avg_session_duration = Column(Float, nullable=True)
    # In seconds
    pages_per_session = Column(Float, nullable=True)

    # Conversion metrics
    conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4TrafficSource {self.session_source}/{self.session_medium} - {self.date}>"


class GA4LandingPage(Base):
    """Landing page performance (daily)"""
    __tablename__ = "ga4_landing_pages"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Landing page
    landing_page = Column(String, index=True, nullable=False)
    # Page path (e.g., /products/bathroom-sink)

    # Source/Medium
    session_source = Column(String, index=True, nullable=True)
    session_medium = Column(String, index=True, nullable=True)

    # Traffic metrics
    sessions = Column(Integer, default=0)
    total_users = Column(Integer, default=0)
    new_users = Column(Integer, default=0)

    # Engagement metrics
    bounce_rate = Column(Float, nullable=True)
    avg_session_duration = Column(Float, nullable=True)
    pages_per_session = Column(Float, nullable=True)

    # Conversion metrics
    conversions = Column(Integer, default=0)
    conversion_rate = Column(Float, nullable=True)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4LandingPage {self.landing_page} - {self.date}>"


class GA4ProductPerformance(Base):
    """E-commerce product performance from GA4"""
    __tablename__ = "ga4_products"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Product identification
    item_id = Column(String, index=True, nullable=False)
    # SKU or product ID
    item_name = Column(String, nullable=True)
    item_category = Column(String, nullable=True)

    # Product metrics
    items_viewed = Column(Integer, default=0)
    items_added_to_cart = Column(Integer, default=0)
    items_purchased = Column(Integer, default=0)

    # Revenue metrics
    item_revenue = Column(Numeric(10, 2), default=0)

    # Derived metrics
    add_to_cart_rate = Column(Float, nullable=True)
    # items_added_to_cart / items_viewed
    purchase_rate = Column(Float, nullable=True)
    # items_purchased / items_viewed

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4Product {self.item_name} - {self.date}>"


class GA4ConversionPath(Base):
    """Conversion path data for attribution"""
    __tablename__ = "ga4_conversion_paths"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Conversion path
    conversion_path = Column(JSON, nullable=False)
    # List of touchpoints: [
    #   {"source": "google", "medium": "organic", "timestamp": "..."},
    #   {"source": "facebook", "medium": "cpc", "timestamp": "..."}
    # ]

    path_length = Column(Integer, default=0)
    # Number of touchpoints

    # Conversion metrics
    conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Time to conversion
    days_to_conversion = Column(Integer, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4ConversionPath {self.path_length} touchpoints - {self.date}>"


class GA4Event(Base):
    """GA4 events/conversions (page_view, add_to_cart, purchase, etc.)"""
    __tablename__ = "ga4_events"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Event details
    event_name = Column(String, index=True, nullable=False)
    # e.g., page_view, add_to_cart, purchase, scroll

    event_count = Column(Integer, default=0)
    total_users = Column(Integer, default=0)

    # Revenue (for conversion events like purchase)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Event parameters (stored as JSON)
    event_params = Column(JSON, nullable=True)
    # e.g., {"page_location": "/products/sink", "value": 99.99}

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4Event {self.event_name} - {self.date}>"


class GA4DailyEcommerce(Base):
    """Daily e-commerce totals for Shopify reconciliation"""
    __tablename__ = "ga4_daily_ecommerce"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False, unique=True)

    # E-commerce metrics (use ecommercePurchases for accurate Shopify reconciliation)
    ecommerce_purchases = Column(Integer, default=0)
    # Count of actual purchases (not all conversion events)
    total_revenue = Column(Numeric(10, 2), default=0)
    add_to_carts = Column(Integer, default=0)
    checkouts = Column(Integer, default=0)
    items_viewed = Column(Integer, default=0)

    # Derived metrics
    cart_to_purchase_rate = Column(Float, nullable=True)
    # ecommerce_purchases / add_to_carts

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4DailyEcommerce {self.date} - {self.ecommerce_purchases} purchases>"


class GA4DailySummary(Base):
    """
    Daily site-wide summary metrics.

    Provides a single row per day with aggregate metrics for:
    - Traffic (users, sessions, pageviews)
    - Engagement (bounce rate, session duration, etc.)
    - Conversions (total conversions, revenue)

    This replaces the awkward traffic_overview data that was stored
    in GA4TrafficSource with source='(all)'.
    """
    __tablename__ = "ga4_daily_summary"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True, nullable=False, unique=True)

    # Core traffic metrics
    active_users = Column(Integer, default=0)
    new_users = Column(Integer, default=0)
    returning_users = Column(Integer, default=0)  # active_users - new_users
    sessions = Column(Integer, default=0)
    pageviews = Column(Integer, default=0)  # screenPageViews

    # Engagement metrics
    engaged_sessions = Column(Integer, default=0)
    engagement_rate = Column(Float, nullable=True)  # engaged_sessions / sessions
    bounce_rate = Column(Float, nullable=True)
    avg_session_duration = Column(Float, nullable=True)  # seconds
    avg_engagement_duration = Column(Float, nullable=True)  # userEngagementDuration / users
    pages_per_session = Column(Float, nullable=True)  # pageviews / sessions
    events_per_session = Column(Float, nullable=True)  # total_events / sessions
    total_events = Column(Integer, default=0)

    # Conversion summary
    total_conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(12, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4DailySummary {self.date} - {self.sessions} sessions>"


class GA4DeviceBreakdown(Base):
    """
    Daily metrics by device category (desktop, mobile, tablet).

    Useful for:
    - Mobile vs desktop conversion analysis
    - Device-specific UX issues
    - Traffic trends by device
    """
    __tablename__ = "ga4_device_breakdown"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True, nullable=False)

    # Device dimension
    device_category = Column(String, index=True, nullable=False)  # desktop, mobile, tablet

    # Traffic metrics
    sessions = Column(Integer, default=0)
    active_users = Column(Integer, default=0)
    new_users = Column(Integer, default=0)

    # Engagement
    engaged_sessions = Column(Integer, default=0)
    bounce_rate = Column(Float, nullable=True)
    avg_session_duration = Column(Float, nullable=True)

    # Conversions
    conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4DeviceBreakdown {self.device_category} - {self.date}>"


class GA4GeoBreakdown(Base):
    """
    Daily metrics by geography (country, region, city).

    Useful for:
    - Geographic performance analysis
    - Regional marketing targeting
    - International expansion decisions
    """
    __tablename__ = "ga4_geo_breakdown"
    __table_args__ = (
        UniqueConstraint('date', 'country', 'region', 'city', name='uq_ga4_geo_date_country_region_city'),
    )

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True, nullable=False)

    # Geo dimensions (hierarchical)
    country = Column(String, index=True, nullable=False)
    region = Column(String, index=True, nullable=True)  # State/Province
    city = Column(String, nullable=True)

    # Traffic metrics
    sessions = Column(Integer, default=0)
    active_users = Column(Integer, default=0)
    new_users = Column(Integer, default=0)

    # Engagement
    engaged_sessions = Column(Integer, default=0)
    bounce_rate = Column(Float, nullable=True)

    # Conversions
    conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4GeoBreakdown {self.country}/{self.region} - {self.date}>"


class GA4UserType(Base):
    """
    Daily metrics by user type (new vs returning).

    Useful for:
    - New vs returning user behavior analysis
    - Retention analysis
    - Acquisition vs engagement balance
    """
    __tablename__ = "ga4_user_type"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True, nullable=False)

    # User type dimension
    user_type = Column(String, index=True, nullable=False)  # 'new' or 'returning'

    # Metrics
    users = Column(Integer, default=0)
    sessions = Column(Integer, default=0)
    engaged_sessions = Column(Integer, default=0)
    pageviews = Column(Integer, default=0)
    avg_session_duration = Column(Float, nullable=True)

    # Conversions
    conversions = Column(Integer, default=0)
    total_revenue = Column(Numeric(10, 2), default=0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4UserType {self.user_type} - {self.date}>"


class GA4PagePerformance(Base):
    """
    Page-level performance metrics

    Note: GA4 doesn't have a direct "unique pageviews" metric like Universal Analytics.
    The unique_pageviews and entrances fields are derived from sessions for approximation.
    For accurate session-level page analysis, use GA4 segment-based reports.
    """
    __tablename__ = "ga4_pages"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Page
    page_path = Column(String, index=True, nullable=False)
    page_title = Column(String, nullable=True)

    # Traffic metrics
    pageviews = Column(Integer, default=0)
    # screenPageViews from GA4
    unique_pageviews = Column(Integer, default=0)
    # DERIVED: approximated from sessions (GA4 doesn't have unique pageviews)
    entrances = Column(Integer, default=0)
    # DERIVED: approximated from sessions (for landing page entries)
    exits = Column(Integer, default=0)

    # Engagement metrics
    avg_time_on_page = Column(Float, nullable=True)
    # In seconds (averageSessionDuration for page)
    bounce_rate = Column(Float, nullable=True)
    exit_rate = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GA4Page {self.page_path} - {self.date}>"
