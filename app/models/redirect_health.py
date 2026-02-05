"""
404 & Redirect Intelligence Models

Tracks broken links, redirects, and lost revenue from 404 errors.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class NotFoundError(Base):
    """404 error tracking"""
    __tablename__ = "not_found_errors"

    id = Column(Integer, primary_key=True, index=True)

    # URL details
    requested_url = Column(String, index=True, nullable=False)
    # The URL that returned 404

    url_path = Column(String, index=True, nullable=True)
    # Path portion of URL (without domain)

    # Traffic metrics
    total_hits = Column(Integer, default=0, index=True)
    # Total number of 404 hits

    unique_visitors = Column(Integer, default=0)
    # Unique users hitting this 404

    first_seen = Column(DateTime, index=True, nullable=False)
    last_seen = Column(DateTime, index=True, nullable=False)

    # Referrers
    top_referrers = Column(JSON, nullable=True)
    # List of domains/pages linking to this 404
    # e.g., [{"referrer": "google.com", "count": 45}, {"referrer": "instagram.com", "count": 23}]

    external_links_count = Column(Integer, default=0)
    # Count of external sites linking here

    internal_links_count = Column(Integer, default=0)
    # Count of internal pages linking here

    # Traffic source
    source_breakdown = Column(JSON, nullable=True)
    # e.g., {"organic": 120, "direct": 45, "social": 23, "email": 12}

    # User behavior
    avg_session_duration_before_404 = Column(Float, nullable=True)
    # How long users browsed before hitting 404

    bounce_rate_after_404 = Column(Float, nullable=True)
    # % who left site after 404

    # Revenue impact
    estimated_monthly_sessions = Column(Integer, default=0, index=True)
    estimated_conversion_rate = Column(Float, nullable=True)
    # Expected conversion if page existed

    estimated_monthly_revenue_loss = Column(Numeric(10, 2), default=0, index=True)
    # Lost revenue from this 404

    # URL characteristics
    url_type = Column(String, index=True, nullable=True)
    # Types: product_page, collection_page, blog_post, static_page, asset, other

    likely_cause = Column(String, nullable=True)
    # Causes: deleted_product, typo_in_url, old_url_structure, external_bad_link

    # Resolution
    status = Column(String, default="active", index=True)
    # Status: active, fixed, redirect_created, ignored

    recommended_action = Column(String, nullable=True)
    # Actions: create_redirect, fix_internal_links, restore_page, ignore

    redirect_to_url = Column(String, nullable=True)
    # Suggested redirect destination

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<NotFoundError {self.requested_url} ({self.total_hits} hits)>"


class RedirectRule(Base):
    """Redirect configuration and tracking"""
    __tablename__ = "redirect_rules"

    id = Column(Integer, primary_key=True, index=True)

    # Redirect details
    source_url = Column(String, index=True, nullable=False)
    # URL being redirected from

    destination_url = Column(String, index=True, nullable=False)
    # URL being redirected to

    redirect_type = Column(Integer, default=301, index=True)
    # Types: 301 (permanent), 302 (temporary), 307, 308

    # Traffic metrics
    total_hits = Column(Integer, default=0, index=True)
    unique_visitors = Column(Integer, default=0)

    # Performance
    avg_redirect_time_ms = Column(Float, nullable=True)
    # Average redirect response time

    # Chain detection
    is_in_chain = Column(Boolean, default=False, index=True)
    chain_length = Column(Integer, default=1)
    # Number of hops (1 = direct redirect, 2+ = chain)

    final_destination_url = Column(String, nullable=True)
    # Final URL after all redirects

    # Status
    is_active = Column(Boolean, default=True, index=True)
    destination_exists = Column(Boolean, default=True)
    # Does destination URL return 200?

    destination_status_code = Column(Integer, nullable=True)
    # HTTP status of destination (200, 404, etc.)

    # Issues
    has_issues = Column(Boolean, default=False, index=True)
    issues = Column(JSON, nullable=True)
    # e.g., ["redirect_chain", "destination_404", "slow_redirect"]

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_checked = Column(DateTime, nullable=True, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<RedirectRule {self.source_url} → {self.destination_url} ({self.redirect_type})>"


class RedirectChain(Base):
    """Multi-hop redirect detection"""
    __tablename__ = "redirect_chains"

    id = Column(Integer, primary_key=True, index=True)

    # Chain details
    initial_url = Column(String, index=True, nullable=False)
    final_url = Column(String, nullable=False)
    chain_length = Column(Integer, nullable=False, index=True)
    # Number of hops

    chain_path = Column(JSON, nullable=False)
    # Full redirect path
    # e.g., [
    #   {"url": "/old-product", "status": 301},
    #   {"url": "/product-v2", "status": 302},
    #   {"url": "/product-final", "status": 200}
    # ]

    # Performance impact
    total_redirect_time_ms = Column(Float, nullable=True)
    # Total time for all redirects

    # Issues
    severity = Column(String, index=True, nullable=False)
    # Severity: low (2 hops), medium (3-4 hops), high (5+ hops)

    contains_302 = Column(Boolean, default=False)
    # Temporary redirects in chain (bad for SEO)

    ends_in_404 = Column(Boolean, default=False, index=True)
    # Chain ends in 404 (critical issue)

    # Recommendation
    recommended_fix = Column(Text, nullable=True)
    # e.g., "Create direct 301 redirect from /old-product to /product-final"

    # Status
    status = Column(String, default="active", index=True)
    # Status: active, fixed, ignored

    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    fixed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<RedirectChain {self.initial_url} → {self.final_url} ({self.chain_length} hops)>"


class LostRevenue(Base):
    """Revenue impact from 404 errors"""
    __tablename__ = "lost_revenue"

    id = Column(Integer, primary_key=True, index=True)

    # Error reference
    not_found_error_id = Column(Integer, ForeignKey("not_found_errors.id"), nullable=True)
    requested_url = Column(String, index=True, nullable=False)

    # Time period
    analysis_date = Column(Date, index=True, nullable=False)
    period_days = Column(Integer, default=30)

    # Traffic metrics
    total_sessions = Column(Integer, default=0)
    unique_visitors = Column(Integer, default=0)

    # Revenue calculation
    expected_conversion_rate = Column(Float, nullable=True)
    # Based on similar pages or site average

    average_order_value = Column(Numeric(10, 2), nullable=True)
    estimated_lost_conversions = Column(Integer, default=0)
    estimated_lost_revenue = Column(Numeric(10, 2), default=0, index=True)

    # Confidence
    confidence_level = Column(String, nullable=True)
    # Levels: high, medium, low

    calculation_method = Column(String, nullable=True)
    # Methods: similar_page_average, category_average, site_average, historical_data

    # Context
    url_type = Column(String, nullable=True)
    # Types: product, collection, landing_page, blog

    comparable_urls = Column(JSON, nullable=True)
    # Similar URLs used for revenue estimation

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<LostRevenue {self.requested_url} - ${self.estimated_lost_revenue:.2f}>"


class RedirectInsight(Base):
    """LLM-generated redirect and 404 insights"""
    __tablename__ = "redirect_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Analysis scope
    insight_type = Column(String, index=True, nullable=False)
    # Types: 404_analysis, redirect_health, revenue_impact, fix_recommendations

    # LLM-generated content
    executive_summary = Column(Text, nullable=True)
    detailed_analysis = Column(Text, nullable=True)

    top_priorities = Column(JSON, nullable=True)
    # Top 404s to fix by revenue impact

    quick_fixes = Column(JSON, nullable=True)
    # Easy redirects to create

    recommended_redirects = Column(JSON, nullable=True)
    # Specific URL mappings

    # Metrics
    total_404s_found = Column(Integer, default=0)
    high_traffic_404s = Column(Integer, default=0)
    # 404s with >100 sessions/month

    total_redirect_chains = Column(Integer, default=0)
    total_estimated_revenue_loss = Column(Numeric(10, 2), default=0, index=True)

    # Metadata
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    llm_model = Column(String, nullable=True)

    def __repr__(self):
        return f"<RedirectInsight {self.insight_type} at {self.generated_at}>"


class BrokenLink(Base):
    """Internal broken links (links from your site to 404s)"""
    __tablename__ = "broken_links"

    id = Column(Integer, primary_key=True, index=True)

    # Link details
    source_page = Column(String, index=True, nullable=False)
    # Page containing the broken link

    broken_link = Column(String, index=True, nullable=False)
    # The URL that's broken (404)

    link_text = Column(String, nullable=True)
    # Anchor text of the link

    link_type = Column(String, nullable=True)
    # Types: text_link, image_link, nav_link, footer_link

    # Impact
    source_page_traffic = Column(Integer, default=0)
    # Monthly sessions to source page

    link_click_rate = Column(Float, nullable=True)
    # Estimated % of visitors clicking this link

    estimated_monthly_clicks = Column(Integer, default=0)
    # Clicks to this broken link

    # Priority
    priority = Column(String, index=True, nullable=False)
    # Priority: high (high-traffic page), medium, low

    # Status
    status = Column(String, default="active", index=True)
    # Status: active, fixed, ignored

    recommended_fix = Column(String, nullable=True)
    # Fix: remove_link, update_link, create_redirect

    suggested_replacement = Column(String, nullable=True)
    # Suggested URL to replace broken link

    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    fixed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return f"<BrokenLink {self.source_page} → {self.broken_link}>"
