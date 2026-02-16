"""
Competitor Blog Monitoring Models

Tracks blog posts and content from competitors and suppliers
to surface trends, inspiration, and competitive intelligence.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, JSON, Date, Boolean
from datetime import datetime

from app.models.base import Base


class CompetitorSite(Base):
    """Registered competitor/supplier sites to monitor"""
    __tablename__ = "competitor_sites"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    domain = Column(String, unique=True, nullable=False, index=True)
    site_type = Column(String, index=True, nullable=False)
    # Types: competitor, supplier, industry

    # Feed discovery
    blog_url = Column(String, nullable=True)
    feed_url = Column(String, nullable=True)  # RSS/Atom feed URL if available
    feed_type = Column(String, nullable=True)  # rss, atom, sitemap, scrape
    sitemap_url = Column(String, nullable=True)

    # Scraping config
    article_selector = Column(String, nullable=True)  # CSS selector for article links
    title_selector = Column(String, nullable=True)
    date_selector = Column(String, nullable=True)
    content_selector = Column(String, nullable=True)

    is_active = Column(Boolean, default=True, index=True)

    # Stats
    total_articles = Column(Integer, default=0)
    last_scraped_at = Column(DateTime, nullable=True)
    last_new_article_at = Column(DateTime, nullable=True)
    consecutive_failures = Column(Integer, default=0)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CompetitorSite {self.name} ({self.domain})>"


class CompetitorArticle(Base):
    """Individual blog posts/articles scraped from competitor sites"""
    __tablename__ = "competitor_articles"

    id = Column(Integer, primary_key=True, index=True)

    # Source
    site_domain = Column(String, index=True, nullable=False)
    site_name = Column(String, nullable=True)

    # Article data
    url = Column(String, unique=True, nullable=False, index=True)
    title = Column(String, nullable=False)
    excerpt = Column(Text, nullable=True)
    content_text = Column(Text, nullable=True)  # Plain text for analysis
    author = Column(String, nullable=True)
    published_at = Column(DateTime, nullable=True, index=True)
    image_url = Column(String, nullable=True)

    # Categorisation
    categories = Column(JSON, nullable=True)  # ["bathroom", "renovation"]
    tags = Column(JSON, nullable=True)  # ["tips", "trends", "2025"]
    detected_topics = Column(JSON, nullable=True)  # LLM-extracted topics
    detected_products = Column(JSON, nullable=True)  # Product mentions

    # Content metrics
    word_count = Column(Integer, nullable=True)
    has_video = Column(Boolean, default=False)
    has_images = Column(Boolean, default=False)
    image_count = Column(Integer, default=0)

    # Relevance scoring
    relevance_score = Column(Float, default=0, index=True)
    # 0-100: how relevant this is to our business

    # Inspiration tracking
    is_flagged = Column(Boolean, default=False, index=True)
    flag_reason = Column(String, nullable=True)
    inspiration_notes = Column(Text, nullable=True)

    # Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CompetitorArticle '{self.title}' from {self.site_domain}>"
