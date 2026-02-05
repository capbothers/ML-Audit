"""
Google Search Console Data Models

Stores query performance and index coverage data.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class SearchConsoleQuery(Base):
    """Query performance from Search Console"""
    __tablename__ = "search_console_queries"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Query details
    query = Column(String, index=True, nullable=False)
    # Search query
    page = Column(String, index=True, nullable=True)
    # Landing page URL

    # Dimensions
    device = Column(String, nullable=True)
    # Types: DESKTOP, MOBILE, TABLET
    country = Column(String, nullable=True)
    # ISO country code (e.g., US, GB)

    # Performance metrics
    clicks = Column(Integer, default=0, index=True)
    impressions = Column(Integer, default=0, index=True)
    ctr = Column(Float, nullable=True)
    # Click-through rate
    position = Column(Float, nullable=True, index=True)
    # Average position in search results

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<SearchConsoleQuery '{self.query}' - {self.date}>"


class SearchConsolePage(Base):
    """Page-level performance from Search Console"""
    __tablename__ = "search_console_pages"

    id = Column(Integer, primary_key=True, index=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Page
    page = Column(String, index=True, nullable=False)
    # Full URL

    # Dimensions
    device = Column(String, nullable=True)
    country = Column(String, nullable=True)

    # Performance metrics
    clicks = Column(Integer, default=0)
    impressions = Column(Integer, default=0)
    ctr = Column(Float, nullable=True)
    position = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<SearchConsolePage {self.page} - {self.date}>"


class SearchConsoleIndexCoverage(Base):
    """Index coverage status from Search Console"""
    __tablename__ = "search_console_index_coverage"

    id = Column(Integer, primary_key=True, index=True)

    # URL
    url = Column(String, index=True, nullable=False, unique=True)

    # Index status
    indexing_state = Column(String, index=True, nullable=True)
    # States: Submitted and indexed, Discovered - currently not indexed,
    # Crawled - currently not indexed, Excluded by 'noindex' tag, etc.

    coverage_state = Column(String, index=True, nullable=True)
    # States: Valid, Valid with warnings, Error, Excluded

    # Last crawl
    last_crawl_time = Column(DateTime, nullable=True, index=True)
    crawl_status = Column(String, nullable=True)
    # Status: Success, Redirect, Error

    # Verdict
    verdict = Column(String, nullable=True)
    # Verdict: PASS, PARTIAL, FAIL, NEUTRAL

    # Issues
    crawl_errors = Column(JSON, nullable=True)
    # List of crawl errors if any

    robots_txt_state = Column(String, nullable=True)
    # ALLOWED, DISALLOWED

    # Metadata
    last_checked = Column(DateTime, nullable=True, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<SearchConsoleIndexCoverage {self.url} - {self.indexing_state}>"


class SearchConsoleSitemap(Base):
    """Sitemap status from Search Console"""
    __tablename__ = "search_console_sitemaps"

    id = Column(Integer, primary_key=True, index=True)

    # Sitemap
    sitemap_url = Column(String, index=True, nullable=False, unique=True)

    # Status
    is_pending = Column(Boolean, default=False)
    is_sitemaps_index = Column(Boolean, default=False)

    # Coverage
    submitted_urls = Column(Integer, default=0)
    indexed_urls = Column(Integer, default=0)

    # Errors/warnings
    errors = Column(Integer, default=0)
    warnings = Column(Integer, default=0)

    # Last update
    last_submitted = Column(DateTime, nullable=True)
    last_downloaded = Column(DateTime, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<SearchConsoleSitemap {self.sitemap_url}>"


class SearchConsoleRichResult(Base):
    """Rich results (structured data) from Search Console"""
    __tablename__ = "search_console_rich_results"

    id = Column(Integer, primary_key=True, index=True)

    # Page
    page = Column(String, index=True, nullable=False)

    # Rich result type
    rich_result_type = Column(String, index=True, nullable=True)
    # Types: Product, Article, Recipe, FAQ, etc.

    # Status
    status = Column(String, index=True, nullable=True)
    # Status: Valid, Valid with warnings, Error

    # Items detected
    items_detected = Column(Integer, default=0)
    items_valid = Column(Integer, default=0)
    items_with_warnings = Column(Integer, default=0)
    items_with_errors = Column(Integer, default=0)

    # Issues
    issues = Column(JSON, nullable=True)
    # List of structured data issues

    # Metadata
    last_checked = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<SearchConsoleRichResult {self.page} - {self.rich_result_type}>"
