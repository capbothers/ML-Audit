"""
Content & Merchandising Gap Analysis Models

Identifies missing content, merchandising issues, and optimization opportunities.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class ContentGap(Base):
    """Identified content gaps (missing descriptions, images, etc.)"""
    __tablename__ = "content_gaps"

    id = Column(Integer, primary_key=True, index=True)

    # Product/Page identification
    product_id = Column(String, index=True, nullable=True)
    product_title = Column(String, nullable=True)
    product_handle = Column(String, index=True, nullable=True)
    page_url = Column(String, nullable=True)
    category = Column(String, index=True, nullable=True)

    # Gap details
    gap_type = Column(String, index=True, nullable=False)
    # Types: missing_description, short_description, missing_images, poor_images,
    # missing_size_guide, missing_specs, missing_video, missing_reviews, etc.

    gap_severity = Column(String, index=True, nullable=False)
    # Severity: critical, high, medium, low

    current_state = Column(JSON, nullable=True)
    # e.g., {"description_length": 50, "image_count": 1, "has_video": false}

    expected_state = Column(JSON, nullable=True)
    # e.g., {"description_length": 300, "image_count": 5, "has_video": true}

    # Impact metrics
    monthly_traffic = Column(Integer, default=0, index=True)
    current_conversion_rate = Column(Float, nullable=True)
    expected_conversion_rate = Column(Float, nullable=True)
    estimated_revenue_impact = Column(Numeric(10, 2), default=0, index=True)
    # Estimated monthly revenue gain if gap is fixed

    # Effort estimation
    effort_hours = Column(Float, nullable=True)
    required_resources = Column(JSON, nullable=True)
    # e.g., ["copywriter", "photographer", "developer"]

    # Priority
    priority_score = Column(Float, default=0, index=True)
    # Score = (revenue_impact / effort_hours)
    priority_level = Column(String, index=True, nullable=True)
    # Levels: critical, high, medium, low

    # Status
    status = Column(String, default="identified", index=True)
    # Status: identified, in_progress, completed, deferred

    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ContentGap {self.gap_type} on {self.product_handle or self.page_url}>"


class MerchandisingGap(Base):
    """Merchandising issues (missing cross-sells, poor categorization, etc.)"""
    __tablename__ = "merchandising_gaps"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_id = Column(String, index=True, nullable=True)
    product_title = Column(String, nullable=True)
    product_handle = Column(String, index=True, nullable=True)
    category = Column(String, index=True, nullable=True)

    # Gap details
    gap_type = Column(String, index=True, nullable=False)
    # Types: missing_cross_sells, missing_upsells, poor_categorization,
    # no_bundle_opportunities, missing_related_products, wrong_collection,
    # missing_product_badges, poor_product_positioning

    gap_description = Column(Text, nullable=True)

    # Current vs Expected
    current_state = Column(JSON, nullable=True)
    # e.g., {"cross_sells": 0, "category": "Other", "badges": []}

    recommended_state = Column(JSON, nullable=True)
    # e.g., {"cross_sells": ["Product A", "Product B"], "category": "Bathroom Sinks"}

    # Impact
    monthly_revenue = Column(Numeric(10, 2), default=0)
    missed_cross_sell_revenue = Column(Numeric(10, 2), default=0, index=True)
    estimated_impact = Column(Numeric(10, 2), default=0, index=True)

    # Effort
    effort_level = Column(String, nullable=True)
    # Levels: low, medium, high
    effort_hours = Column(Float, nullable=True)

    # Priority
    priority_score = Column(Float, default=0, index=True)
    priority_level = Column(String, index=True, nullable=True)

    # Status
    status = Column(String, default="identified", index=True)

    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<MerchandisingGap {self.gap_type} on {self.product_handle}>"


class ContentOpportunity(Base):
    """High-impact content to create (new guides, videos, etc.)"""
    __tablename__ = "content_opportunities"

    id = Column(Integer, primary_key=True, index=True)

    # Opportunity details
    opportunity_type = Column(String, index=True, nullable=False)
    # Types: buying_guide, how_to_video, comparison_chart, size_guide,
    # installation_guide, care_instructions, FAQ_section, blog_post, landing_page

    topic = Column(String, nullable=False)
    # e.g., "Bathroom Sink Buying Guide", "How to Install Faucets"

    target_audience = Column(String, nullable=True)
    # e.g., "first-time home buyers", "DIY enthusiasts", "contractors"

    content_description = Column(Text, nullable=True)

    # Current state
    exists = Column(Boolean, default=False)
    current_content_url = Column(String, nullable=True)
    current_quality_score = Column(Integer, nullable=True)
    # Score 0-100

    # Opportunity metrics
    search_volume = Column(Integer, default=0, index=True)
    # Monthly searches for this topic

    keyword_difficulty = Column(Integer, nullable=True)
    # SEO difficulty 0-100

    estimated_monthly_traffic = Column(Integer, default=0, index=True)
    estimated_conversion_rate = Column(Float, nullable=True)
    estimated_monthly_revenue = Column(Numeric(10, 2), default=0, index=True)

    # Competition
    competitor_has_content = Column(Boolean, default=False)
    competitor_examples = Column(JSON, nullable=True)
    # List of competitor URLs with this content

    # Effort
    effort_hours = Column(Float, nullable=True)
    required_resources = Column(JSON, nullable=True)
    estimated_cost = Column(Numeric(10, 2), default=0)

    # Priority
    priority_score = Column(Float, default=0, index=True)
    # Score = (estimated_revenue * conversion_rate) / effort_hours
    priority_level = Column(String, index=True, nullable=True)

    # Recommended approach
    content_format = Column(JSON, nullable=True)
    # e.g., ["long-form blog post", "video", "infographic"]

    outline = Column(JSON, nullable=True)
    # Suggested content outline/structure

    target_keywords = Column(JSON, nullable=True)
    # Keywords to target in content

    # Status
    status = Column(String, default="identified", index=True)
    # Status: identified, planned, in_progress, published, declined

    # Metadata
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ContentOpportunity {self.topic}>"


class ContentPerformance(Base):
    """Content effectiveness metrics (traffic vs conversion)"""
    __tablename__ = "content_performance"

    id = Column(Integer, primary_key=True, index=True)

    # Content identification
    page_url = Column(String, index=True, nullable=False)
    page_title = Column(String, nullable=True)
    content_type = Column(String, index=True, nullable=True)
    # Types: product_page, category_page, blog_post, guide, landing_page, video

    product_id = Column(String, index=True, nullable=True)
    product_handle = Column(String, index=True, nullable=True)

    # Traffic metrics
    monthly_sessions = Column(Integer, default=0, index=True)
    monthly_pageviews = Column(Integer, default=0)
    avg_time_on_page = Column(Float, nullable=True)
    # In seconds
    bounce_rate = Column(Float, nullable=True)

    # Engagement metrics
    scroll_depth_avg = Column(Float, nullable=True)
    # Average % of page scrolled
    video_play_rate = Column(Float, nullable=True)
    # % of visitors who played video

    # Conversion metrics
    conversion_rate = Column(Float, nullable=True, index=True)
    monthly_conversions = Column(Integer, default=0)
    monthly_revenue = Column(Numeric(10, 2), default=0, index=True)

    # Content quality indicators
    readability_score = Column(Integer, nullable=True)
    # 0-100 (Flesch reading ease)
    word_count = Column(Integer, nullable=True)
    image_count = Column(Integer, nullable=True)
    video_count = Column(Integer, nullable=True)
    has_cta = Column(Boolean, default=False)

    # Performance classification
    performance_category = Column(String, index=True, nullable=True)
    # Categories: high_traffic_low_conversion (optimization opportunity),
    # high_traffic_high_conversion (working well),
    # low_traffic_high_conversion (promote more),
    # low_traffic_low_conversion (fix or remove)

    # Benchmarks
    benchmark_conversion_rate = Column(Float, nullable=True)
    vs_benchmark = Column(Float, nullable=True)
    # Percentage above/below benchmark

    # Optimization potential
    estimated_optimized_conversion_rate = Column(Float, nullable=True)
    estimated_revenue_gain = Column(Numeric(10, 2), default=0, index=True)

    # Metadata
    analysis_date = Column(Date, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<ContentPerformance {self.page_url}>"


class ContentInsight(Base):
    """LLM-generated content recommendations"""
    __tablename__ = "content_insights"

    id = Column(Integer, primary_key=True, index=True)

    # Analysis scope
    insight_type = Column(String, index=True, nullable=False)
    # Types: gap_analysis, opportunity_analysis, performance_analysis,
    # merchandising_analysis, competitive_analysis

    analysis_focus = Column(String, nullable=True)
    # e.g., "Bathroom Sinks Category", "Product XYZ", "All Products"

    # LLM-generated content
    executive_summary = Column(Text, nullable=True)
    # High-level summary of findings

    top_priorities = Column(JSON, nullable=True)
    # Top 3-5 content/merchandising priorities

    detailed_analysis = Column(Text, nullable=True)
    # Full LLM analysis

    quick_wins = Column(JSON, nullable=True)
    # Low-effort, high-impact improvements

    strategic_recommendations = Column(JSON, nullable=True)
    # Longer-term content strategy

    # Metrics
    total_gaps_found = Column(Integer, default=0)
    total_opportunities = Column(Integer, default=0)
    estimated_total_impact = Column(Numeric(10, 2), default=0, index=True)

    # Metadata
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    llm_model = Column(String, nullable=True)

    def __repr__(self):
        return f"<ContentInsight {self.insight_type} at {self.generated_at}>"


class CategoryContentHealth(Base):
    """Content health score by category"""
    __tablename__ = "category_content_health"

    id = Column(Integer, primary_key=True, index=True)

    # Category
    category_name = Column(String, index=True, nullable=False)
    product_count = Column(Integer, default=0)

    # Content completeness scores (0-100)
    description_score = Column(Integer, nullable=True)
    # % of products with good descriptions

    image_score = Column(Integer, nullable=True)
    # % of products with 4+ images

    seo_score = Column(Integer, nullable=True)
    # % of products with meta descriptions, alt text, etc.

    merchandising_score = Column(Integer, nullable=True)
    # % of products with cross-sells, correct categorization, etc.

    overall_health_score = Column(Integer, nullable=True, index=True)
    # Weighted average of all scores

    # Gap summary
    total_gaps = Column(Integer, default=0)
    critical_gaps = Column(Integer, default=0)

    # Performance
    avg_conversion_rate = Column(Float, nullable=True)
    total_monthly_revenue = Column(Numeric(10, 2), default=0)

    # Potential
    estimated_revenue_if_optimized = Column(Numeric(10, 2), default=0)
    revenue_opportunity = Column(Numeric(10, 2), default=0, index=True)

    # Metadata
    analysis_date = Column(Date, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<CategoryContentHealth {self.category_name} - {self.overall_health_score}/100>"


class BlogDraft(Base):
    """LLM-generated blog post drafts based on SEO underperformers"""
    __tablename__ = "blog_drafts"

    id = Column(Integer, primary_key=True, index=True)

    # SEO trigger data
    source_query = Column(String, index=True, nullable=False)
    source_page = Column(String, nullable=True)
    opportunity_type = Column(String, index=True, nullable=False)
    # Types: close_to_page_one, declining, high_impression_low_ctr, content_decay, manual

    # SEO metrics snapshot at generation time
    position_at_generation = Column(Float, nullable=True)
    impressions_at_generation = Column(Integer, nullable=True)
    clicks_at_generation = Column(Integer, nullable=True)
    click_gap_at_generation = Column(Integer, nullable=True)
    priority_score = Column(Integer, nullable=True)

    # Generated content
    title = Column(String, nullable=False)
    meta_description = Column(String(300), nullable=True)
    slug = Column(String, nullable=True)
    content_html = Column(Text, nullable=False)
    outline = Column(JSON, nullable=True)
    target_keywords = Column(JSON, nullable=True)
    internal_links = Column(JSON, nullable=True)
    word_count = Column(Integer, nullable=True)
    estimated_reading_time = Column(Integer, nullable=True)

    # Status workflow
    status = Column(String, default="draft", index=True)
    # Status: draft, reviewed, approved, published, rejected
    reviewer_notes = Column(Text, nullable=True)

    # LLM metadata
    llm_model = Column(String, nullable=True)
    generation_tokens = Column(Integer, nullable=True)

    # Timestamps
    generated_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    reviewed_at = Column(DateTime, nullable=True)
    published_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<BlogDraft '{self.title}' ({self.status})>"
