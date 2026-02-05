"""
Customer data models
Unified customer data from Shopify and Klaviyo
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, JSON
from sqlalchemy.sql import func
from app.models.base import Base


class Customer(Base):
    """Unified customer model"""
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)  # Shopify customer ID
    email = Column(String, index=True)
    first_name = Column(String)
    last_name = Column(String)

    # Purchase behavior
    total_spent = Column(Float, default=0.0)
    orders_count = Column(Integer, default=0)
    average_order_value = Column(Float, default=0.0)

    # Engagement
    accepts_marketing = Column(Boolean, default=False)
    klaviyo_engaged = Column(Boolean, default=False)
    last_order_date = Column(DateTime)
    last_email_open_date = Column(DateTime)

    # Churn prediction
    churn_score = Column(Float, nullable=True)  # ML-generated churn probability
    churn_risk_level = Column(String, nullable=True)  # LOW, MEDIUM, HIGH
    predicted_ltv = Column(Float, nullable=True)  # Lifetime value prediction

    # Metadata
    source = Column(String)  # shopify, klaviyo
    state = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Additional data
    tags = Column(JSON)
    custom_attributes = Column(JSON)
