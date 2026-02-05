"""
Shopify Data Models

Stores data pulled from Shopify Admin API.
Source of truth for orders, products, and customers.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Boolean, Text, BigInteger, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from datetime import datetime
from decimal import Decimal

from app.models.base import Base


class ShopifyOrder(Base):
    """
    Shopify orders - source of truth for conversions

    Synced from Shopify Admin API: GET /admin/api/2024-01/orders.json
    """
    __tablename__ = "shopify_orders"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify IDs
    shopify_order_id = Column(BigInteger, unique=True, index=True, nullable=False)  # Shopify's order ID
    order_number = Column(Integer, index=True)  # Human-readable order number

    # Customer
    customer_id = Column(BigInteger, index=True, nullable=True)  # Shopify customer ID
    customer_email = Column(String, index=True, nullable=True)

    # Order status
    financial_status = Column(String, index=True)  # paid, pending, refunded, partially_refunded
    fulfillment_status = Column(String, index=True, nullable=True)  # fulfilled, partial, null

    # Amounts (all in store currency)
    currency = Column(String, default='AUD')
    total_price = Column(Numeric(10, 2))  # Original order value (gross)
    current_total_price = Column(Numeric(10, 2), nullable=True)  # Current value after refunds (net)
    subtotal_price = Column(Numeric(10, 2))  # Before tax and shipping
    current_subtotal_price = Column(Numeric(10, 2), nullable=True)  # Current subtotal after refunds (excl tax/shipping)
    total_tax = Column(Numeric(10, 2))
    total_discounts = Column(Numeric(10, 2), default=0)
    total_shipping = Column(Numeric(10, 2), default=0)

    # Refunds
    total_refunded = Column(Numeric(10, 2), default=0)
    refund_count = Column(Integer, default=0)

    # Line items (stored as JSON for flexibility)
    line_items = Column(JSON)  # [{product_id, variant_id, sku, quantity, price, title}, ...]

    # Discounts
    discount_codes = Column(JSON, nullable=True)  # [{code, amount, type}, ...]

    # Attribution / UTM data
    landing_site = Column(Text, nullable=True)  # Full landing URL with UTM params
    referring_site = Column(Text, nullable=True)  # Referrer
    source_name = Column(String, index=True, nullable=True)  # shopify, web, pos

    # Parsed UTM parameters
    utm_source = Column(String, index=True, nullable=True)
    utm_medium = Column(String, index=True, nullable=True)
    utm_campaign = Column(String, index=True, nullable=True)
    utm_term = Column(String, nullable=True)
    utm_content = Column(String, nullable=True)

    # Google Ads attribution (parsed from landing_site)
    gclid = Column(String, index=True, nullable=True)
    gad_campaign_id = Column(String, index=True, nullable=True)

    # Customer location
    shipping_country = Column(String, nullable=True)
    shipping_province = Column(String, nullable=True)
    shipping_city = Column(String, nullable=True)
    shipping_zip = Column(String, nullable=True)

    # Tags (for segmentation)
    tags = Column(JSON, nullable=True)  # ["wholesale", "vip", etc.]

    # Timestamps
    created_at = Column(DateTime, index=True)  # When order was placed
    updated_at = Column(DateTime, index=True)  # When order was last modified
    cancelled_at = Column(DateTime, nullable=True, index=True)
    processed_at = Column(DateTime, nullable=True)

    # Sync metadata
    synced_at = Column(DateTime, default=datetime.utcnow)


class ShopifyProduct(Base):
    """
    Shopify products catalog

    Synced from Shopify Admin API: GET /admin/api/2024-01/products.json
    """
    __tablename__ = "shopify_products"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify IDs
    shopify_product_id = Column(BigInteger, unique=True, index=True, nullable=False)
    handle = Column(String, index=True)  # URL-friendly identifier

    # Product info
    title = Column(String, nullable=False)
    body_html = Column(Text, nullable=True)  # Product description
    vendor = Column(String, index=True, nullable=True)
    product_type = Column(String, index=True, nullable=True)
    tags = Column(JSON, nullable=True)  # ["bathroom", "vanity", etc.]

    # Status
    status = Column(String, index=True)  # active, archived, draft

    # Variants (stored as JSON)
    # Each variant has: id, sku, price, compare_at_price, inventory_quantity
    variants = Column(JSON)  # [{id, sku, price, compare_at_price, inventory_quantity, title}, ...]

    # Images
    images = Column(JSON, nullable=True)  # [{src, alt}, ...]
    featured_image = Column(String, nullable=True)

    # SEO
    seo_title = Column(String, nullable=True)
    seo_description = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, index=True)
    updated_at = Column(DateTime, index=True)
    published_at = Column(DateTime, nullable=True)

    # Sync metadata
    synced_at = Column(DateTime, default=datetime.utcnow)


class ShopifyCustomer(Base):
    """
    Shopify customers

    Synced from Shopify Admin API: GET /admin/api/2024-01/customers.json
    """
    __tablename__ = "shopify_customers"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify ID
    shopify_customer_id = Column(BigInteger, unique=True, index=True, nullable=False)

    # Customer info
    email = Column(String, index=True, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    phone = Column(String, nullable=True)

    # Customer metrics
    orders_count = Column(Integer, default=0, index=True)
    total_spent = Column(Numeric(10, 2), default=0, index=True)

    # Customer status
    state = Column(String, index=True)  # enabled, disabled, invited, declined
    verified_email = Column(Boolean, default=False)
    accepts_marketing = Column(Boolean, default=False)
    marketing_opt_in_level = Column(String, nullable=True)

    # Tags (for segmentation)
    tags = Column(JSON, nullable=True)  # ["vip", "wholesale", etc.]

    # Location (from default address)
    default_address_city = Column(String, nullable=True)
    default_address_province = Column(String, nullable=True)
    default_address_country = Column(String, nullable=True)
    default_address_zip = Column(String, nullable=True)

    # Timestamps
    created_at = Column(DateTime, index=True)  # First order date
    updated_at = Column(DateTime, index=True)
    last_order_date = Column(DateTime, nullable=True, index=True)

    # Churn prediction (calculated fields)
    days_since_last_order = Column(Integer, nullable=True, index=True)
    is_at_risk = Column(Boolean, default=False, index=True)
    churn_probability = Column(Float, nullable=True)

    # Sync metadata
    synced_at = Column(DateTime, default=datetime.utcnow)


class ShopifyOrderItem(Base):
    """
    Normalized order line items for fast product analytics.

    Denormalizes line_items JSON from ShopifyOrder for:
    - Fast product mix queries by date
    - Product performance over time
    - SKU-level analytics
    """
    __tablename__ = "shopify_order_items"

    id = Column(Integer, primary_key=True, index=True)

    # Order reference
    shopify_order_id = Column(BigInteger, ForeignKey('shopify_orders.shopify_order_id'), index=True, nullable=False)
    order_number = Column(Integer, index=True)

    # Denormalized order date for fast date-range queries
    order_date = Column(DateTime, index=True, nullable=False)

    # Product identifiers
    shopify_product_id = Column(BigInteger, index=True, nullable=True)
    shopify_variant_id = Column(BigInteger, index=True, nullable=True)
    sku = Column(String, index=True, nullable=True)

    # Product info
    title = Column(String, nullable=True)
    variant_title = Column(String, nullable=True)
    vendor = Column(String, index=True, nullable=True)
    product_type = Column(String, index=True, nullable=True)

    # Quantities and amounts
    quantity = Column(Integer, nullable=False, default=1)
    price = Column(Numeric(10, 2), nullable=False)  # Unit price
    total_price = Column(Numeric(10, 2), nullable=False)  # quantity * price
    total_discount = Column(Numeric(10, 2), default=0)

    # For profitability (if available)
    cost_per_item = Column(Numeric(10, 2), nullable=True)  # COGS per unit

    # Order context (denormalized for fast filtering)
    financial_status = Column(String, index=True, nullable=True)
    fulfillment_status = Column(String, nullable=True)

    # Sync metadata
    synced_at = Column(DateTime, default=datetime.utcnow)

    # Relationship
    order = relationship("ShopifyOrder", backref="items", foreign_keys=[shopify_order_id],
                         primaryjoin="ShopifyOrderItem.shopify_order_id == ShopifyOrder.shopify_order_id")


class ShopifyRefund(Base):
    """
    Shopify refunds

    Synced from Shopify Admin API: GET /admin/api/2024-01/orders/{id}/refunds.json
    Used for calculating return rates and true product profitability
    """
    __tablename__ = "shopify_refunds"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify IDs
    shopify_refund_id = Column(BigInteger, unique=True, index=True, nullable=False)
    shopify_order_id = Column(BigInteger, index=True, nullable=False)  # Parent order

    # Refund details
    refund_line_items = Column(JSON)  # [{line_item_id, quantity, subtotal, product_id, sku}, ...]

    # Amounts
    total_refunded = Column(Numeric(10, 2))
    currency = Column(String, default='AUD')

    # Reason
    note = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, index=True)
    processed_at = Column(DateTime, nullable=True)

    # Sync metadata
    synced_at = Column(DateTime, default=datetime.utcnow)


class ShopifyInventory(Base):
    """
    Shopify inventory levels

    Tracks stock levels for inventory management
    Optional - can sync if needed for stock alerts
    """
    __tablename__ = "shopify_inventory"

    id = Column(Integer, primary_key=True, index=True)

    # Shopify IDs
    shopify_inventory_item_id = Column(BigInteger, unique=True, index=True, nullable=False)
    shopify_product_id = Column(BigInteger, index=True)
    shopify_variant_id = Column(BigInteger, index=True)

    # Product info
    sku = Column(String, index=True)
    title = Column(String)
    vendor = Column(String, nullable=True)

    # Inventory
    inventory_quantity = Column(Integer, default=0)
    inventory_policy = Column(String)  # deny, continue (allow overselling)

    # Cost (if available from Shopify)
    cost = Column(Numeric(10, 2), nullable=True)

    # Timestamps
    updated_at = Column(DateTime, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow)
