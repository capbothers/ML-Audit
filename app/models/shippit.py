"""
Shippit Shipping Data Models

Stores actual fulfillment cost data from Shippit, linked to Shopify orders.
Enables per-order shipping cost in net margin calculation.
"""
from sqlalchemy import Column, Integer, String, DateTime, JSON, Numeric, BigInteger
from datetime import datetime
from app.models.base import Base


class ShippitOrder(Base):
    """
    Shippit order/shipment — actual fulfillment cost per Shopify order.

    Synced from Shippit API: GET /orders
    Linked to ShopifyOrder via retailer_order_number -> order_number.
    """
    __tablename__ = "shippit_orders"

    id = Column(Integer, primary_key=True, index=True)

    # Shippit identifiers
    tracking_number = Column(String, unique=True, index=True, nullable=False)

    # Link to Shopify
    retailer_order_number = Column(String, index=True, nullable=True)
    shopify_order_id = Column(BigInteger, index=True, nullable=True)

    # Carrier and service
    courier_name = Column(String, nullable=True)
    courier_type = Column(String, nullable=True)
    service_level = Column(String, nullable=True)

    # Cost (the critical field)
    shipping_cost = Column(Numeric(10, 2), nullable=True)
    currency = Column(String, default='AUD')

    # Status
    state = Column(String, index=True, nullable=True)

    # Parcel details
    parcel_count = Column(Integer, default=1)

    # Raw response for debugging
    raw_response = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, index=True, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    synced_at = Column(DateTime, default=datetime.utcnow)
