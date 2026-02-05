"""
Transaction and order models
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, ForeignKey, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.models.base import Base


class Order(Base):
    """Order/transaction model"""
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)  # Shopify order ID
    order_number = Column(String, index=True)

    # Customer relationship
    customer_id = Column(Integer, ForeignKey("customers.id"), index=True)
    customer_email = Column(String, index=True)

    # Financial data
    total_price = Column(Float)
    subtotal_price = Column(Float)
    total_tax = Column(Float)
    total_discounts = Column(Float, default=0.0)
    currency = Column(String)

    # Order status
    financial_status = Column(String)
    fulfillment_status = Column(String)

    # Attribution
    source_name = Column(String, index=True)
    referring_site = Column(Text)
    landing_site = Column(Text)
    utm_source = Column(String)
    utm_medium = Column(String)
    utm_campaign = Column(String)

    # Items
    line_items_count = Column(Integer)
    line_items = Column(JSON)

    # Timestamps
    order_date = Column(DateTime, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class AbandonedCheckout(Base):
    """Abandoned checkout model - critical for conversion optimization"""
    __tablename__ = "abandoned_checkouts"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    token = Column(String, index=True)

    customer_email = Column(String, index=True)
    total_price = Column(Float)
    currency = Column(String)

    # Recovery
    abandoned_checkout_url = Column(Text)
    recovered = Column(Boolean, default=False)
    recovery_email_sent = Column(Boolean, default=False)

    # Items
    line_items = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), index=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    completed_at = Column(DateTime, nullable=True)
