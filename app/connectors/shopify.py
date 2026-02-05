"""
Shopify Connector

Syncs data from Shopify Admin API.
Source of truth for orders, products, and customers.
"""
import httpx
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time

from app.connectors.base import BaseConnector
from app.models.shopify import (
    ShopifyOrder,
    ShopifyProduct,
    ShopifyCustomer,
    ShopifyRefund,
    ShopifyOrderItem
)
from app.utils.logger import log


class ShopifyConnector(BaseConnector):
    """
    Connector for Shopify Admin API

    Syncs orders, products, customers, and refunds
    """

    def __init__(
        self,
        db: Session,
        store_url: str,
        access_token: str,
        api_version: str = "2024-01"
    ):
        """
        Initialize Shopify connector

        Args:
            db: Database session
            store_url: Shopify store URL (e.g., "your-store.myshopify.com")
            access_token: Shopify Admin API access token
            api_version: API version to use
        """
        super().__init__(db, source_name="shopify", source_type="ecommerce")

        self.store_url = store_url.replace('https://', '').replace('http://', '')
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"https://{self.store_url}/admin/api/{api_version}"

        # Rate limiting
        self.requests_per_second = 2  # Shopify: 2 req/sec
        self.last_request_time = 0

    async def authenticate(self) -> bool:
        """
        Test Shopify authentication

        Returns:
            True if authenticated successfully
        """
        try:
            # Test with a simple shop query
            url = f"{self.base_url}/shop.json"
            headers = self._get_headers()

            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, timeout=30.0)

                if response.status_code == 200:
                    shop_data = response.json()
                    log.info(f"Authenticated with Shopify store: {shop_data['shop']['name']}")
                    self._authenticated = True
                    return True
                else:
                    log.error(f"Shopify authentication failed: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            log.error(f"Error authenticating with Shopify: {str(e)}")
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Run full incremental sync

        Args:
            start_date: Start date (if None, use last successful sync)
            end_date: End date (if None, use now)

        Returns:
            Dict with sync results
        """
        sync_start = time.time()

        try:
            await self.log_sync_start()

            # Authenticate if not already
            if not self.is_authenticated():
                authenticated = await self.authenticate()
                if not authenticated:
                    await self.log_sync_failure("Authentication failed")
                    return {"success": False, "error": "Authentication failed"}

            # Determine sync window
            if not start_date:
                last_sync = await self.get_last_successful_sync()
                start_date = last_sync if last_sync else datetime.utcnow() - timedelta(days=90)

            if not end_date:
                end_date = datetime.utcnow()

            log.info(f"Syncing Shopify data from {start_date} to {end_date}")

            # Sync in order of dependencies
            results = {
                "orders": await self.sync_orders(start_date, end_date),
                "products": await self.sync_products(start_date),
                "customers": await self.sync_customers(start_date),
                "refunds": await self.sync_refunds(start_date, end_date)
            }

            # Calculate totals
            total_records = sum(r["synced"] for r in results.values())
            total_errors = sum(r["errors"] for r in results.values())

            sync_duration = time.time() - sync_start

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(
                f"Shopify sync complete: {total_records} records in {sync_duration:.1f}s "
                f"({total_errors} errors)"
            )

            return {
                "success": True,
                "records_synced": total_records,
                "duration_seconds": sync_duration,
                "details": results
            }

        except Exception as e:
            log.error(f"Shopify sync failed: {str(e)}")
            await self.log_sync_failure(str(e))
            return {"success": False, "error": str(e)}

    async def sync_orders(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, int]:
        """
        Sync orders from Shopify

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dict with synced/error counts
        """
        log.info(f"Syncing Shopify orders from {start_date} to {end_date}")

        synced = 0
        errors = 0

        try:
            # Fetch orders with pagination
            url = f"{self.base_url}/orders.json"
            params = {
                "status": "any",  # Get all orders (open, closed, cancelled)
                "updated_at_min": start_date.isoformat(),
                "updated_at_max": end_date.isoformat(),
                "limit": 250  # Max per page
            }

            while url:
                await self._rate_limit()

                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        url,
                        params=params if params else None,
                        headers=self._get_headers(),
                        timeout=60.0
                    )

                    if response.status_code != 200:
                        log.error(f"Error fetching orders: {response.status_code} - {response.text}")
                        errors += 1
                        break

                    data = response.json()
                    orders = data.get("orders", [])

                    # Process each order
                    for order_data in orders:
                        try:
                            await self._save_order(order_data)
                            synced += 1
                        except Exception as e:
                            log.error(f"Error saving order {order_data.get('id')}: {str(e)}")
                            errors += 1

                    # Get next page from Link header
                    url = self._get_next_page_url(response.headers.get("Link"))
                    params = None  # Params are in the URL for subsequent pages

            log.info(f"Synced {synced} orders ({errors} errors)")

            return {"synced": synced, "errors": errors}

        except Exception as e:
            log.error(f"Error syncing orders: {str(e)}")
            return {"synced": synced, "errors": errors + 1}

    async def sync_products(self, start_date: datetime) -> Dict[str, int]:
        """
        Sync products from Shopify

        Args:
            start_date: Only sync products updated since this date

        Returns:
            Dict with synced/error counts
        """
        log.info(f"Syncing Shopify products updated since {start_date}")

        synced = 0
        errors = 0

        try:
            url = f"{self.base_url}/products.json"
            params = {
                "updated_at_min": start_date.isoformat(),
                "limit": 250
            }

            while url:
                await self._rate_limit()

                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        url,
                        params=params if params else None,
                        headers=self._get_headers(),
                        timeout=60.0
                    )

                    if response.status_code != 200:
                        log.error(f"Error fetching products: {response.status_code}")
                        errors += 1
                        break

                    data = response.json()
                    products = data.get("products", [])

                    for product_data in products:
                        try:
                            await self._save_product(product_data)
                            synced += 1
                        except Exception as e:
                            log.error(f"Error saving product {product_data.get('id')}: {str(e)}")
                            errors += 1

                    url = self._get_next_page_url(response.headers.get("Link"))
                    params = None

            log.info(f"Synced {synced} products ({errors} errors)")

            return {"synced": synced, "errors": errors}

        except Exception as e:
            log.error(f"Error syncing products: {str(e)}")
            return {"synced": synced, "errors": errors + 1}

    async def sync_customers(self, start_date: datetime) -> Dict[str, int]:
        """
        Sync customers from Shopify

        Args:
            start_date: Only sync customers updated since this date

        Returns:
            Dict with synced/error counts
        """
        log.info(f"Syncing Shopify customers updated since {start_date}")

        synced = 0
        errors = 0

        try:
            url = f"{self.base_url}/customers.json"
            params = {
                "updated_at_min": start_date.isoformat(),
                "limit": 250
            }

            while url:
                await self._rate_limit()

                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        url,
                        params=params if params else None,
                        headers=self._get_headers(),
                        timeout=60.0
                    )

                    if response.status_code != 200:
                        log.error(f"Error fetching customers: {response.status_code}")
                        errors += 1
                        break

                    data = response.json()
                    customers = data.get("customers", [])

                    for customer_data in customers:
                        try:
                            await self._save_customer(customer_data)
                            synced += 1
                        except Exception as e:
                            log.error(f"Error saving customer {customer_data.get('id')}: {str(e)}")
                            errors += 1

                    url = self._get_next_page_url(response.headers.get("Link"))
                    params = None

            log.info(f"Synced {synced} customers ({errors} errors)")

            return {"synced": synced, "errors": errors}

        except Exception as e:
            log.error(f"Error syncing customers: {str(e)}")
            return {"synced": synced, "errors": errors + 1}

    async def sync_refunds(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, int]:
        """
        Sync refunds for orders in date range

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            Dict with synced/error counts
        """
        log.info(f"Syncing refunds for orders from {start_date} to {end_date}")

        synced = 0
        errors = 0

        try:
            # Get orders in date range that might have refunds
            orders = self.db.query(ShopifyOrder).filter(
                ShopifyOrder.created_at >= start_date,
                ShopifyOrder.created_at <= end_date
            ).all()

            for order in orders:
                try:
                    # Fetch refunds for this order
                    url = f"{self.base_url}/orders/{order.shopify_order_id}/refunds.json"

                    await self._rate_limit()

                    async with httpx.AsyncClient() as client:
                        response = await client.get(
                            url,
                            headers=self._get_headers(),
                            timeout=30.0
                        )

                        if response.status_code == 200:
                            data = response.json()
                            refunds = data.get("refunds", [])

                            for refund_data in refunds:
                                await self._save_refund(refund_data, order.shopify_order_id)
                                synced += 1

                        elif response.status_code != 404:  # 404 = no refunds, which is fine
                            log.error(f"Error fetching refunds for order {order.shopify_order_id}: {response.status_code}")
                            errors += 1

                except Exception as e:
                    log.error(f"Error syncing refunds for order {order.shopify_order_id}: {str(e)}")
                    errors += 1

            log.info(f"Synced {synced} refunds ({errors} errors)")

            return {"synced": synced, "errors": errors}

        except Exception as e:
            log.error(f"Error syncing refunds: {str(e)}")
            return {"synced": synced, "errors": errors + 1}

    async def _save_order(self, order_data: Dict) -> ShopifyOrder:
        """Save or update order in database"""

        # Parse UTM parameters from landing_site
        utm_params = self._parse_utm_params(order_data.get("landing_site"))

        # Check if order exists
        order = self.db.query(ShopifyOrder).filter(
            ShopifyOrder.shopify_order_id == order_data["id"]
        ).first()

        if not order:
            order = ShopifyOrder(shopify_order_id=order_data["id"])
            self.db.add(order)

        # Update fields
        order.order_number = order_data.get("order_number")
        order.customer_id = order_data.get("customer", {}).get("id")
        order.customer_email = order_data.get("customer", {}).get("email") or order_data.get("email")

        order.financial_status = order_data.get("financial_status")
        order.fulfillment_status = order_data.get("fulfillment_status")

        order.currency = order_data.get("currency", "AUD")
        order.total_price = order_data.get("total_price")
        order.current_total_price = order_data.get("current_total_price") or order_data.get("total_price")
        order.subtotal_price = order_data.get("subtotal_price")
        order.total_tax = order_data.get("total_tax")
        order.total_discounts = order_data.get("total_discounts", 0)
        order.total_shipping = order_data.get("total_shipping", 0)

        # Calculate refund total
        refunds = order_data.get("refunds", [])
        order.refund_count = len(refunds)
        order.total_refunded = sum(float(r.get("total_refunded_set", {}).get("shop_money", {}).get("amount", 0)) for r in refunds)

        # Line items
        order.line_items = [
            {
                "product_id": item.get("product_id"),
                "variant_id": item.get("variant_id"),
                "sku": item.get("sku"),
                "title": item.get("title"),
                "quantity": item.get("quantity"),
                "price": item.get("price")
            }
            for item in order_data.get("line_items", [])
        ]

        # Discount codes
        order.discount_codes = order_data.get("discount_codes", [])

        # Attribution
        order.landing_site = order_data.get("landing_site")
        order.referring_site = order_data.get("referring_site")
        order.source_name = order_data.get("source_name")

        order.utm_source = utm_params.get("utm_source")
        order.utm_medium = utm_params.get("utm_medium")
        order.utm_campaign = utm_params.get("utm_campaign")
        order.utm_term = utm_params.get("utm_term")
        order.utm_content = utm_params.get("utm_content")

        # Shipping address
        shipping = order_data.get("shipping_address", {})
        order.shipping_country = shipping.get("country")
        order.shipping_province = shipping.get("province")
        order.shipping_city = shipping.get("city")
        order.shipping_zip = shipping.get("zip")

        # Tags
        tags_str = order_data.get("tags", "")
        order.tags = tags_str.split(", ") if tags_str else []

        # Timestamps
        order.created_at = self._normalize_date(order_data.get("created_at"))
        order.updated_at = self._normalize_date(order_data.get("updated_at"))
        order.cancelled_at = self._normalize_date(order_data.get("cancelled_at"))
        order.processed_at = self._normalize_date(order_data.get("processed_at"))

        order.synced_at = datetime.utcnow()

        self.db.commit()

        # Save normalized order items for fast product analytics
        await self._save_order_items(order, order_data.get("line_items", []))

        return order

    async def _save_order_items(self, order: ShopifyOrder, line_items: List[Dict]) -> None:
        """
        Save normalized order line items for fast product analytics.
        Enables queries like 'product mix by date' without parsing JSON.
        """
        if not line_items:
            return

        # Delete existing items for this order (upsert pattern)
        self.db.query(ShopifyOrderItem).filter(
            ShopifyOrderItem.shopify_order_id == order.shopify_order_id
        ).delete()

        # Insert new items
        for item in line_items:
            order_item = ShopifyOrderItem(
                shopify_order_id=order.shopify_order_id,
                order_number=order.order_number,
                order_date=order.created_at,

                shopify_product_id=item.get("product_id"),
                shopify_variant_id=item.get("variant_id"),
                sku=item.get("sku"),

                title=item.get("title"),
                variant_title=item.get("variant_title"),
                vendor=item.get("vendor"),
                product_type=item.get("product_type"),

                quantity=item.get("quantity", 1),
                price=item.get("price", 0),
                total_price=float(item.get("price", 0)) * int(item.get("quantity", 1)),
                total_discount=float(item.get("total_discount", 0)),

                financial_status=order.financial_status,
                fulfillment_status=order.fulfillment_status,

                synced_at=datetime.utcnow()
            )
            self.db.add(order_item)

        self.db.commit()

    async def _save_product(self, product_data: Dict) -> ShopifyProduct:
        """Save or update product in database"""

        product = self.db.query(ShopifyProduct).filter(
            ShopifyProduct.shopify_product_id == product_data["id"]
        ).first()

        if not product:
            product = ShopifyProduct(shopify_product_id=product_data["id"])
            self.db.add(product)

        # Update fields
        product.handle = product_data.get("handle")
        product.title = product_data.get("title")
        product.body_html = product_data.get("body_html")
        product.vendor = product_data.get("vendor")
        product.product_type = product_data.get("product_type")

        tags_str = product_data.get("tags", "")
        product.tags = tags_str.split(", ") if tags_str else []

        product.status = product_data.get("status")

        # Variants
        product.variants = [
            {
                "id": v.get("id"),
                "sku": v.get("sku"),
                "title": v.get("title"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "inventory_quantity": v.get("inventory_quantity")
            }
            for v in product_data.get("variants", [])
        ]

        # Images
        images = product_data.get("images", [])
        product.images = [{"src": img.get("src"), "alt": img.get("alt")} for img in images]
        product.featured_image = images[0].get("src") if images else None

        # Timestamps
        product.created_at = self._normalize_date(product_data.get("created_at"))
        product.updated_at = self._normalize_date(product_data.get("updated_at"))
        product.published_at = self._normalize_date(product_data.get("published_at"))

        product.synced_at = datetime.utcnow()

        self.db.commit()

        return product

    async def _save_customer(self, customer_data: Dict) -> ShopifyCustomer:
        """Save or update customer in database"""

        customer = self.db.query(ShopifyCustomer).filter(
            ShopifyCustomer.shopify_customer_id == customer_data["id"]
        ).first()

        if not customer:
            customer = ShopifyCustomer(shopify_customer_id=customer_data["id"])
            self.db.add(customer)

        # Update fields
        customer.email = customer_data.get("email")
        customer.first_name = customer_data.get("first_name")
        customer.last_name = customer_data.get("last_name")
        customer.phone = customer_data.get("phone")

        customer.orders_count = customer_data.get("orders_count", 0)
        customer.total_spent = customer_data.get("total_spent", 0)

        customer.state = customer_data.get("state")
        customer.verified_email = customer_data.get("verified_email", False)
        customer.accepts_marketing = customer_data.get("accepts_marketing", False)
        customer.marketing_opt_in_level = customer_data.get("marketing_opt_in_level")

        tags_str = customer_data.get("tags", "")
        customer.tags = tags_str.split(", ") if tags_str else []

        # Default address
        address = customer_data.get("default_address", {})
        customer.default_address_city = address.get("city")
        customer.default_address_province = address.get("province")
        customer.default_address_country = address.get("country")
        customer.default_address_zip = address.get("zip")

        # Timestamps
        customer.created_at = self._normalize_date(customer_data.get("created_at"))
        customer.updated_at = self._normalize_date(customer_data.get("updated_at"))

        # Last order date (from Shopify if available)
        last_order_date = customer_data.get("last_order_date")
        if last_order_date:
            customer.last_order_date = self._normalize_date(last_order_date)

        # Calculate days since last order
        if customer.last_order_date:
            customer.days_since_last_order = (datetime.utcnow() - customer.last_order_date).days

        customer.synced_at = datetime.utcnow()

        self.db.commit()

        return customer

    async def _save_refund(self, refund_data: Dict, order_id: int) -> ShopifyRefund:
        """Save or update refund in database"""

        refund = self.db.query(ShopifyRefund).filter(
            ShopifyRefund.shopify_refund_id == refund_data["id"]
        ).first()

        if not refund:
            refund = ShopifyRefund(shopify_refund_id=refund_data["id"])
            self.db.add(refund)

        refund.shopify_order_id = order_id

        # Refund line items
        refund.refund_line_items = [
            {
                "line_item_id": item.get("line_item_id"),
                "quantity": item.get("quantity"),
                "subtotal": item.get("subtotal"),
                "line_item": item.get("line_item", {})
            }
            for item in refund_data.get("refund_line_items", [])
        ]

        # Total
        transactions = refund_data.get("transactions", [])
        refund.total_refunded = sum(float(t.get("amount", 0)) for t in transactions)
        refund.currency = transactions[0].get("currency", "AUD") if transactions else "AUD"

        refund.note = refund_data.get("note")

        # Timestamps
        refund.created_at = self._normalize_date(refund_data.get("created_at"))
        refund.processed_at = self._normalize_date(refund_data.get("processed_at"))

        refund.synced_at = datetime.utcnow()

        self.db.commit()

        return refund

    def _parse_utm_params(self, landing_site: Optional[str]) -> Dict[str, Optional[str]]:
        """
        Parse UTM parameters from landing site URL

        Args:
            landing_site: Full URL with query parameters

        Returns:
            Dict with utm_source, utm_medium, etc.
        """
        if not landing_site:
            return {
                "utm_source": None,
                "utm_medium": None,
                "utm_campaign": None,
                "utm_term": None,
                "utm_content": None
            }

        try:
            parsed = urlparse(landing_site)
            params = parse_qs(parsed.query)

            return {
                "utm_source": params.get("utm_source", [None])[0],
                "utm_medium": params.get("utm_medium", [None])[0],
                "utm_campaign": params.get("utm_campaign", [None])[0],
                "utm_term": params.get("utm_term", [None])[0],
                "utm_content": params.get("utm_content", [None])[0]
            }

        except Exception as e:
            log.warning(f"Error parsing UTM params from {landing_site}: {str(e)}")
            return {
                "utm_source": None,
                "utm_medium": None,
                "utm_campaign": None,
                "utm_term": None,
                "utm_content": None
            }

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Shopify API requests"""
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json"
        }

    def _get_next_page_url(self, link_header: Optional[str]) -> Optional[str]:
        """
        Parse next page URL from Link header

        Shopify uses cursor-based pagination with Link headers

        Args:
            link_header: Link header from response

        Returns:
            Next page URL or None
        """
        if not link_header:
            return None

        # Parse Link header: <url>; rel="next"
        links = link_header.split(",")
        for link in links:
            parts = link.split(";")
            if len(parts) == 2 and 'rel="next"' in parts[1]:
                url = parts[0].strip().strip("<>")
                return url

        return None

    async def _rate_limit(self):
        """
        Enforce rate limiting (2 requests per second for Shopify)
        """
        now = time.time()
        elapsed = now - self.last_request_time

        if elapsed < (1.0 / self.requests_per_second):
            sleep_time = (1.0 / self.requests_per_second) - elapsed
            time.sleep(sleep_time)

        self.last_request_time = time.time()
