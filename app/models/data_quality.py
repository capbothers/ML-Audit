"""
Data Quality & Tracking Validation Models

Ensures data integrity across all sources.
Answers: "Can I trust this data?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text
from datetime import datetime

from app.models.base import Base


class DataSyncStatus(Base):
    """
    Track data sync status for each data source

    Monitors last sync time, success/failure, error messages
    """
    __tablename__ = "data_sync_status"

    id = Column(Integer, primary_key=True, index=True)

    # Data source
    source_name = Column(String, unique=True, index=True)  # shopify, ga4, google_ads, klaviyo, etc.
    source_type = Column(String)  # ecommerce, analytics, advertising, email, feed

    # Sync status
    last_sync_attempt = Column(DateTime, index=True)
    last_successful_sync = Column(DateTime, index=True, nullable=True)
    sync_status = Column(String, index=True)  # success, failed, in_progress, stale

    # Sync metrics
    records_synced = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    sync_duration_seconds = Column(Float, nullable=True)

    # Error tracking
    last_error = Column(Text, nullable=True)
    error_count = Column(Integer, default=0)  # Consecutive errors
    first_error_at = Column(DateTime, nullable=True)

    # Data freshness
    latest_data_timestamp = Column(DateTime, nullable=True)  # Most recent data point synced
    data_lag_hours = Column(Float, nullable=True)  # How far behind is the data?

    # Health indicators
    is_healthy = Column(Boolean, default=True, index=True)
    health_score = Column(Integer, default=100)  # 0-100
    health_issues = Column(JSON, nullable=True)  # List of issues

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TrackingDiscrepancy(Base):
    """
    Daily conversion tracking comparison across platforms

    Compares Shopify (truth) vs GA4 vs Google Ads
    """
    __tablename__ = "tracking_discrepancies"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(DateTime, index=True)

    # Shopify (source of truth)
    shopify_orders = Column(Integer)
    shopify_revenue = Column(Float)

    # GA4
    ga4_conversions = Column(Integer, nullable=True)
    ga4_revenue = Column(Float, nullable=True)
    ga4_discrepancy_pct = Column(Float, nullable=True)  # % difference from Shopify
    ga4_missing_conversions = Column(Integer, nullable=True)

    # Google Ads
    google_ads_conversions = Column(Integer, nullable=True)
    google_ads_revenue = Column(Float, nullable=True)
    google_ads_discrepancy_pct = Column(Float, nullable=True)
    google_ads_missing_conversions = Column(Integer, nullable=True)

    # Klaviyo (if tracking)
    klaviyo_conversions = Column(Integer, nullable=True)
    klaviyo_revenue = Column(Float, nullable=True)
    klaviyo_discrepancy_pct = Column(Float, nullable=True)

    # Overall health
    has_critical_discrepancy = Column(Boolean, default=False, index=True)  # >20% off
    has_warning_discrepancy = Column(Boolean, default=False, index=True)  # 10-20% off

    # Diagnosis
    probable_cause = Column(Text, nullable=True)  # LLM-generated diagnosis
    discrepancy_started_at = Column(DateTime, nullable=True)  # When did this issue begin?
    is_resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class UTMHealthCheck(Base):
    """
    UTM parameter tracking health

    Monitors UTM coverage and consistency
    """
    __tablename__ = "utm_health_checks"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    date = Column(DateTime, index=True)
    period_type = Column(String)  # daily, weekly

    # Coverage metrics
    total_sessions = Column(Integer)
    sessions_with_utm = Column(Integer)
    utm_coverage_pct = Column(Float)  # % of sessions with UTM params

    # By source
    utm_by_source = Column(JSON)  # Coverage by traffic source

    # Quality issues
    missing_utm_campaigns = Column(JSON, nullable=True)  # Campaigns with no UTMs
    inconsistent_naming = Column(JSON, nullable=True)  # UTM naming pattern issues
    malformed_utms = Column(Integer, default=0)  # Count of malformed UTM strings

    # Health indicators
    is_healthy = Column(Boolean, default=True, index=True)
    health_score = Column(Integer, default=100)  # 0-100
    issues_detected = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)


class MerchantCenterHealth(Base):
    """
    Google Merchant Center feed health

    Tracks product feed status and issues
    """
    __tablename__ = "merchant_center_health"

    id = Column(Integer, primary_key=True, index=True)

    # Check timestamp
    checked_at = Column(DateTime, index=True)

    # Feed status
    last_feed_upload = Column(DateTime, nullable=True)
    feed_processing_status = Column(String, nullable=True)  # success, failed, processing

    # Product counts
    total_products = Column(Integer, default=0)
    approved_products = Column(Integer, default=0)
    disapproved_products = Column(Integer, default=0)
    pending_products = Column(Integer, default=0)
    expiring_products = Column(Integer, default=0)

    # Disapproval details
    disapproval_reasons = Column(JSON, nullable=True)  # Breakdown by reason
    new_disapprovals = Column(Integer, default=0)  # Since last check
    new_disapproval_products = Column(JSON, nullable=True)  # Product IDs

    # Feed issues
    feed_warnings = Column(Integer, default=0)
    feed_errors = Column(Integer, default=0)
    issue_summary = Column(JSON, nullable=True)

    # Health indicators
    is_healthy = Column(Boolean, default=True, index=True)
    health_score = Column(Integer, default=100)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)


class GoogleAdsLinkHealth(Base):
    """
    Google Ads / Analytics linking health

    Verifies proper setup and data flow
    """
    __tablename__ = "google_ads_link_health"

    id = Column(Integer, primary_key=True, index=True)

    # Check timestamp
    checked_at = Column(DateTime, index=True)

    # Auto-tagging
    auto_tagging_enabled = Column(Boolean, default=False)
    gclid_present_in_urls = Column(Boolean, default=False)

    # GA4 linking
    ga4_linked = Column(Boolean, default=False)
    ga4_property_id = Column(String, nullable=True)
    ga4_data_sharing_enabled = Column(Boolean, default=False)

    # Conversion import
    conversion_import_enabled = Column(Boolean, default=False)
    conversions_importing = Column(Boolean, default=False)
    last_conversion_import = Column(DateTime, nullable=True)

    # Audience sharing
    audience_sharing_enabled = Column(Boolean, default=False)
    remarketing_enabled = Column(Boolean, default=False)

    # Issues detected
    issues = Column(JSON, nullable=True)
    critical_issues_count = Column(Integer, default=0)

    # Health indicators
    is_healthy = Column(Boolean, default=True, index=True)
    health_score = Column(Integer, default=100)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)


class DataQualityScore(Base):
    """
    Weekly overall data quality score

    Aggregates all data quality metrics into single score
    """
    __tablename__ = "data_quality_scores"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    week_start = Column(DateTime, index=True)
    week_end = Column(DateTime)

    # Overall score (0-100)
    overall_score = Column(Integer, index=True)
    previous_week_score = Column(Integer, nullable=True)
    score_change = Column(Integer, nullable=True)

    # Component scores
    sync_health_score = Column(Integer)  # All sources syncing properly
    tracking_accuracy_score = Column(Integer)  # Conversion tracking alignment
    utm_health_score = Column(Integer)  # UTM parameter coverage
    feed_health_score = Column(Integer)  # Merchant Center health
    link_health_score = Column(Integer)  # GA4/Ads linking health

    # Score breakdown
    score_factors = Column(JSON)  # Detailed breakdown of what affects score

    # Issues summary
    critical_issues_count = Column(Integer, default=0)
    warning_issues_count = Column(Integer, default=0)
    total_issues_count = Column(Integer, default=0)

    # Top issues
    top_issues = Column(JSON, nullable=True)  # Top 5 issues by impact

    # Health status
    status = Column(String, index=True)  # excellent (90+), good (70-89), warning (50-69), critical (<50)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)


class TrackingAlert(Base):
    """
    Data quality alerts

    Alerts for critical data quality issues
    """
    __tablename__ = "tracking_alerts"

    id = Column(Integer, primary_key=True, index=True)

    # Deduplication hash (alert_type + source_name)
    # Used to prevent duplicate alerts and enable efficient lookups
    dedup_hash = Column(String(32), index=True, nullable=True)

    # Alert details
    alert_type = Column(String, index=True)  # sync_failure, discrepancy, utm_drop, feed_issue, link_broken
    severity = Column(String, index=True)  # critical, high, medium, low
    title = Column(String)
    description = Column(Text)

    # Related entity
    source_name = Column(String, nullable=True)  # Which data source
    affected_date = Column(DateTime, nullable=True)  # When did issue occur

    # Issue details
    issue_data = Column(JSON, nullable=True)  # Full details
    probable_cause = Column(Text, nullable=True)  # LLM diagnosis
    recommended_actions = Column(JSON, nullable=True)  # What to do

    # Impact
    estimated_impact = Column(Text, nullable=True)  # Business impact description
    affected_records = Column(Integer, nullable=True)

    # Status
    status = Column(String, default='active', index=True)  # active, acknowledged, investigating, resolved, false_positive
    acknowledged_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    resolution_notes = Column(Text, nullable=True)

    # Alert delivery
    notification_sent = Column(Boolean, default=False)
    notification_channels = Column(JSON, nullable=True)  # email, slack

    # Delivery attempt tracking (for audit trail)
    delivery_attempts = Column(Integer, default=0)  # Total attempts across all channels
    delivery_total_delay_seconds = Column(Float, default=0.0)  # Total retry delay
    delivery_results = Column(JSON, nullable=True)  # Per-channel: {channel: {success, attempts, errors}}

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ValidationFailure(Base):
    """
    Track data validation failures during sync operations.

    Records individual field-level validation issues for audit and debugging.
    Answers: "Why did this record fail to save?" and "What patterns of bad data exist?"
    """
    __tablename__ = "validation_failures"

    id = Column(Integer, primary_key=True, index=True)

    # Link to sync operation
    sync_log_id = Column(Integer, index=True, nullable=True)  # FK to data_sync_logs

    # Entity identification
    entity_type = Column(String, index=True)  # order, customer, product, campaign, etc.
    entity_id = Column(String, index=True)  # External ID from source system
    source = Column(String, index=True)  # shopify, klaviyo, ga4, etc.

    # Validation failure details
    field_name = Column(String, index=True)  # The field that failed validation
    failure_type = Column(String, index=True)  # missing_required, invalid_type, out_of_range, invalid_format, referential_integrity
    failure_message = Column(Text)  # Human-readable error message

    # Raw values for debugging
    raw_value = Column(Text, nullable=True)  # The actual value that failed
    expected_format = Column(String, nullable=True)  # What was expected

    # Context
    validation_rule = Column(String, nullable=True)  # Which rule failed (e.g., "price_non_negative")
    severity = Column(String, default="warning", index=True)  # error (blocked save), warning (saved anyway)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class ValidationRule(Base):
    """
    Configurable validation rules for data quality.

    Allows customizing validation behavior per entity type and field.
    """
    __tablename__ = "validation_rules"

    id = Column(Integer, primary_key=True, index=True)

    # Rule identification
    rule_name = Column(String, unique=True, index=True)  # e.g., "shopify_order_price_positive"
    entity_type = Column(String, index=True)  # order, customer, product
    source = Column(String, index=True)  # shopify, klaviyo, ga4
    field_name = Column(String)  # The field this rule applies to

    # Rule configuration
    rule_type = Column(String)  # required, type_check, range, regex, custom
    rule_config = Column(JSON)  # Rule-specific configuration
    # Examples:
    # required: {"allow_empty": false}
    # type_check: {"expected_type": "number"}
    # range: {"min": 0, "max": 1000000}
    # regex: {"pattern": "^[A-Z]{2}[0-9]+$"}

    # Behavior
    severity = Column(String, default="warning")  # error (block save) or warning (log and continue)
    is_active = Column(Boolean, default=True, index=True)

    # Metadata
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
