"""
Google Ads Data Models

Stores campaign performance, ad groups, products, and click data from Google Ads.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, JSON, ForeignKey, Date, Numeric, BigInteger
from sqlalchemy.orm import relationship
from datetime import datetime

from app.models.base import Base


class GoogleAdsCampaign(Base):
    """Google Ads campaign performance (daily)"""
    __tablename__ = "google_ads_campaigns"

    id = Column(Integer, primary_key=True, index=True)

    # Campaign identification
    campaign_id = Column(String, index=True, nullable=False)
    campaign_name = Column(String, nullable=False)
    campaign_type = Column(String, nullable=True)
    # Types: SEARCH, SHOPPING, DISPLAY, VIDEO, etc.

    campaign_status = Column(String, nullable=True)
    # Status: ENABLED, PAUSED, REMOVED

    # Date
    date = Column(Date, index=True, nullable=False)

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    # Cost in micros (divide by 1,000,000 for actual cost)

    conversions = Column(Float, default=0.0)
    conversions_value = Column(Float, default=0.0)

    # Derived metrics
    ctr = Column(Float, nullable=True)
    # Click-through rate
    avg_cpc = Column(Float, nullable=True)
    # Average cost per click
    conversion_rate = Column(Float, nullable=True)

    # Impression share metrics
    search_impression_share = Column(Float, nullable=True)
    search_budget_lost_impression_share = Column(Float, nullable=True)
    search_rank_lost_impression_share = Column(Float, nullable=True)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GoogleAdsCampaign {self.campaign_name} - {self.date}>"


class GoogleAdsAdGroup(Base):
    """Google Ads ad group performance (daily)"""
    __tablename__ = "google_ads_ad_groups"

    id = Column(Integer, primary_key=True, index=True)

    # Ad group identification
    ad_group_id = Column(String, index=True, nullable=False)
    ad_group_name = Column(String, nullable=False)
    campaign_id = Column(String, index=True, nullable=False)
    ad_group_status = Column(String, nullable=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Float, default=0.0)
    conversions_value = Column(Float, default=0.0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GoogleAdsAdGroup {self.ad_group_name} - {self.date}>"


class GoogleAdsProductPerformance(Base):
    """Shopping/Product performance from Google Ads (daily)"""
    __tablename__ = "google_ads_products"

    id = Column(Integer, primary_key=True, index=True)

    # Product identification
    product_item_id = Column(String, index=True, nullable=False)
    product_title = Column(String, nullable=True)

    campaign_id = Column(String, index=True, nullable=False)
    campaign_name = Column(String, nullable=True)
    ad_group_id = Column(String, nullable=True)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Float, default=0.0)
    conversions_value = Column(Float, default=0.0)

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GoogleAdsProduct {self.product_item_id} - {self.date}>"


class GoogleAdsSearchTerm(Base):
    """Search terms report from Google Ads"""
    __tablename__ = "google_ads_search_terms"

    id = Column(Integer, primary_key=True, index=True)

    # Search term
    search_term = Column(String, index=True, nullable=False)
    campaign_id = Column(String, index=True, nullable=False)
    ad_group_id = Column(String, index=True, nullable=False)

    # Date
    date = Column(Date, index=True, nullable=False)

    # Performance metrics
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    cost_micros = Column(BigInteger, default=0)
    conversions = Column(Float, default=0.0)
    conversions_value = Column(Float, default=0.0)

    # Match type
    keyword_match_type = Column(String, nullable=True)
    # Types: EXACT, PHRASE, BROAD

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GoogleAdsSearchTerm {self.search_term} - {self.date}>"


class GoogleAdsClick(Base):
    """Click data for attribution (GCLID tracking)"""
    __tablename__ = "google_ads_clicks"

    id = Column(Integer, primary_key=True, index=True)

    # Click identification
    gclid = Column(String, unique=True, index=True, nullable=False)
    # Google Click ID

    click_date = Column(DateTime, index=True, nullable=False)
    campaign_id = Column(String, index=True, nullable=False)
    ad_group_id = Column(String, nullable=True)
    keyword_id = Column(String, nullable=True)

    # Click details
    device = Column(String, nullable=True)
    # Types: MOBILE, DESKTOP, TABLET

    ad_network_type = Column(String, nullable=True)
    # Types: SEARCH, CONTENT, YOUTUBE_SEARCH, YOUTUBE_WATCH

    # Metadata
    synced_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<GoogleAdsClick {self.gclid}>"
