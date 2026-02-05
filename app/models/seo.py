"""
SEO Intelligence Models

Tracks Search Console data and SEO opportunities.
Answers: "Where are my easy SEO wins?"
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, Date
from datetime import datetime

from app.models.base import Base


class SearchQuery(Base):
    """
    Search Console query performance data

    Tracks how keywords perform in search results
    """
    __tablename__ = "search_queries"

    id = Column(Integer, primary_key=True, index=True)

    # Query details
    query = Column(String, index=True)
    date = Column(Date, index=True)

    # Country/device (optional filters)
    country = Column(String, nullable=True)
    device = Column(String, nullable=True)  # desktop, mobile, tablet

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    ctr = Column(Float, default=0.0)  # Click-through rate (%)
    position = Column(Float, default=0.0)  # Average position

    # Period comparison
    impressions_previous = Column(Integer, nullable=True)  # Previous period
    clicks_previous = Column(Integer, nullable=True)
    ctr_previous = Column(Float, nullable=True)
    position_previous = Column(Float, nullable=True)

    # Changes
    impressions_change_pct = Column(Float, nullable=True)
    clicks_change_pct = Column(Float, nullable=True)
    position_change = Column(Float, nullable=True)

    # Opportunity flags
    is_high_impression_low_ctr = Column(Boolean, default=False, index=True)  # Ranking but not clicking
    is_close_to_page_one = Column(Boolean, default=False, index=True)  # Position 8-15
    is_declining = Column(Boolean, default=False, index=True)  # Losing traffic

    # Opportunity score (0-100)
    opportunity_score = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PageSEO(Base):
    """
    Page-level SEO performance and issues

    Tracks how individual pages perform in search
    """
    __tablename__ = "page_seo"

    id = Column(Integer, primary_key=True, index=True)

    # Page details
    url = Column(String, unique=True, index=True)
    page_type = Column(String, nullable=True)  # product, collection, blog, page
    date = Column(Date, index=True)

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    ctr = Column(Float, default=0.0)
    position = Column(Float, default=0.0)

    # Period comparison
    impressions_previous = Column(Integer, nullable=True)
    clicks_previous = Column(Integer, nullable=True)
    ctr_previous = Column(Float, nullable=True)
    position_previous = Column(Float, nullable=True)

    # Changes
    impressions_change_pct = Column(Float, nullable=True)
    clicks_change_pct = Column(Float, nullable=True)
    position_change = Column(Float, nullable=True)

    # Top queries for this page
    top_queries = Column(JSON, nullable=True)  # Top 10 queries driving traffic

    # Technical SEO
    is_indexed = Column(Boolean, default=True)
    indexing_issues = Column(JSON, nullable=True)
    mobile_usable = Column(Boolean, default=True)
    mobile_issues = Column(JSON, nullable=True)

    # Core Web Vitals
    lcp_score = Column(Float, nullable=True)  # Largest Contentful Paint (seconds)
    fid_score = Column(Float, nullable=True)  # First Input Delay (ms)
    cls_score = Column(Float, nullable=True)  # Cumulative Layout Shift
    core_web_vitals_pass = Column(Boolean, nullable=True)

    # Content quality signals
    title_tag = Column(String, nullable=True)
    meta_description = Column(String, nullable=True)
    h1_tag = Column(String, nullable=True)
    word_count = Column(Integer, nullable=True)
    image_count = Column(Integer, nullable=True)

    # Opportunity flags
    is_declining = Column(Boolean, default=False, index=True)
    has_technical_issues = Column(Boolean, default=False, index=True)
    has_content_gaps = Column(Boolean, default=False, index=True)

    # Opportunity score
    opportunity_score = Column(Integer, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SEOOpportunity(Base):
    """
    Identified SEO opportunities

    Specific actionable SEO improvements ranked by impact
    """
    __tablename__ = "seo_opportunities"

    id = Column(Integer, primary_key=True, index=True)

    # Opportunity details
    opportunity_type = Column(String, index=True)  # low_ctr, close_to_page_one, declining_page, technical_issue
    priority = Column(String, index=True)  # critical, high, medium, low

    # What this is about
    title = Column(String)
    description = Column(Text)

    # Related entity
    query = Column(String, nullable=True)  # If query-related
    url = Column(String, nullable=True)  # If page-related

    # Current state
    current_position = Column(Float, nullable=True)
    current_ctr = Column(Float, nullable=True)
    current_clicks = Column(Integer, nullable=True)
    current_impressions = Column(Integer, nullable=True)

    # Potential
    estimated_clicks_gain = Column(Integer, nullable=True)
    estimated_traffic_increase_pct = Column(Float, nullable=True)
    effort_level = Column(String, nullable=True)  # low, medium, high

    # Recommendations
    recommended_action = Column(Text)
    specific_steps = Column(JSON, nullable=True)  # List of steps
    expected_timeline = Column(String, nullable=True)  # "1-2 weeks", "1-3 months"

    # Impact score (0-100)
    impact_score = Column(Integer, default=0, index=True)

    # LLM-generated
    llm_analysis = Column(Text, nullable=True)

    # Status
    status = Column(String, default='open', index=True)  # open, in_progress, completed, dismissed
    acknowledged_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class IndexCoverage(Base):
    """
    Google Search Console index coverage status

    Tracks which pages are indexed vs excluded
    """
    __tablename__ = "index_coverage"

    id = Column(Integer, primary_key=True, index=True)

    # Check date
    checked_at = Column(DateTime, index=True)

    # Coverage stats
    valid_pages = Column(Integer, default=0)
    error_pages = Column(Integer, default=0)
    warning_pages = Column(Integer, default=0)
    excluded_pages = Column(Integer, default=0)

    # Error breakdown
    errors_by_type = Column(JSON, nullable=True)
    warnings_by_type = Column(JSON, nullable=True)
    exclusions_by_type = Column(JSON, nullable=True)

    # Specific issues
    server_errors = Column(Integer, default=0)
    not_found_errors = Column(Integer, default=0)
    redirect_errors = Column(Integer, default=0)
    crawl_errors = Column(Integer, default=0)

    # Pages with issues
    error_urls = Column(JSON, nullable=True)  # Sample URLs with errors

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)


class SEOInsight(Base):
    """
    LLM-generated SEO insights and strategic recommendations

    High-level analysis of SEO performance and opportunities
    """
    __tablename__ = "seo_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Time period
    period_start = Column(Date, index=True)
    period_end = Column(Date)

    # Insight type
    insight_type = Column(String, index=True)  # quick_wins, strategic, technical, content

    # Summary
    title = Column(String)
    summary = Column(Text)

    # Analysis
    llm_analysis = Column(Text)

    # Recommendations
    recommended_actions = Column(JSON)  # List of specific actions
    estimated_impact = Column(String, nullable=True)  # Traffic/revenue impact estimate

    # Priority
    priority_score = Column(Integer, default=0, index=True)

    # Related opportunities
    opportunity_ids = Column(JSON, nullable=True)  # IDs of related SEOOpportunity records

    # Status
    is_active = Column(Boolean, default=True, index=True)
    acknowledged = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CoreWebVitals(Base):
    """
    Core Web Vitals performance data

    Tracks page speed and user experience metrics
    """
    __tablename__ = "core_web_vitals"

    id = Column(Integer, primary_key=True, index=True)

    # URL
    url = Column(String, index=True)
    url_pattern = Column(String, nullable=True)  # Grouped URL pattern

    # Check date
    checked_at = Column(DateTime, index=True)

    # Device
    device = Column(String, index=True)  # desktop, mobile

    # Core Web Vitals
    lcp_value = Column(Float)  # Largest Contentful Paint (seconds)
    lcp_status = Column(String)  # good, needs_improvement, poor

    fid_value = Column(Float)  # First Input Delay (ms)
    fid_status = Column(String)

    cls_value = Column(Float)  # Cumulative Layout Shift
    cls_status = Column(String)

    # Overall status
    overall_status = Column(String, index=True)  # pass, fail

    # Pass thresholds
    # LCP: good < 2.5s, poor > 4.0s
    # FID: good < 100ms, poor > 300ms
    # CLS: good < 0.1, poor > 0.25

    # Sample size
    sample_size = Column(Integer, default=0)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
