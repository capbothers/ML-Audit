"""
Shopify data connector
Fetches orders, customers, products, and analytics from Shopify
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
import pytz
import shopify
from dateutil import parser as date_parser
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()

# Sydney timezone for Cass Brothers
SYDNEY_TZ = pytz.timezone('Australia/Sydney')


class ShopifyConnector(BaseConnector):
    """Connector for Shopify e-commerce platform"""

    def __init__(self):
        super().__init__("Shopify")
        self.session = None

    async def connect(self) -> bool:
        """Establish connection to Shopify"""
        try:
            self.session = shopify.Session(
                settings.shopify_shop_url,
                settings.shopify_api_version,
                settings.shopify_access_token
            )
            shopify.ShopifyResource.activate_session(self.session)
            log.info(f"Connected to Shopify: {settings.shopify_shop_url}")
            return True
        except Exception as e:
            log.error(f"Failed to connect to Shopify: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate Shopify connection"""
        try:
            if not self.session:
                await self.connect()
            shop = shopify.Shop.current()
            return shop is not None
        except Exception as e:
            log.error(f"Shopify connection validation failed: {str(e)}")
            return False

    def _to_sydney_time(self, dt: datetime) -> datetime:
        """Convert datetime to Sydney timezone"""
        if dt.tzinfo is None:
            # Assume UTC if naive
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(SYDNEY_TZ)

    def _parse_datetime(self, val) -> Optional[datetime]:
        """Parse datetime from string or return datetime object as-is"""
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        try:
            return date_parser.parse(val)
        except:
            return None

    def _get_sydney_date_range(self, start_date: datetime, end_date: datetime) -> tuple:
        """
        Get proper date range in Sydney timezone.
        Returns ISO format strings suitable for Shopify API.
        """
        # Convert to Sydney timezone
        if start_date.tzinfo is None:
            start_sydney = SYDNEY_TZ.localize(start_date.replace(hour=0, minute=0, second=0, microsecond=0))
        else:
            start_sydney = start_date.astimezone(SYDNEY_TZ)

        if end_date.tzinfo is None:
            end_sydney = SYDNEY_TZ.localize(end_date.replace(hour=23, minute=59, second=59, microsecond=999999))
        else:
            end_sydney = end_date.astimezone(SYDNEY_TZ)

        # Return ISO format with timezone info
        return start_sydney.isoformat(), end_sydney.isoformat()

    async def fetch_data(self, start_date: datetime, end_date: datetime, include_products: bool = True) -> Dict[str, Any]:
        """
        Fetch data from Shopify.

        Args:
            start_date: Start of date range
            end_date: End of date range
            include_products: If False, skip product fetch (much faster for order-only queries)
        """
        if not self.session:
            await self.connect()

        # Log the date range we're fetching
        start_str, end_str = self._get_sydney_date_range(start_date, end_date)
        log.info(f"Fetching Shopify data from {start_str} to {end_str} (Sydney time)")

        if not include_products:
            log.info("Skipping product fetch (orders-only mode)")

        data = {
            "orders": await self._fetch_orders(start_date, end_date),
            "customers": await self._fetch_customers(start_date, end_date),
            "products": await self._fetch_products() if include_products else [],
            "abandoned_checkouts": await self._fetch_abandoned_checkouts(start_date, end_date),
            "shop_info": await self._fetch_shop_info()
        }

        # Calculate accurate revenue (excluding voided/cancelled, using current_total_price)
        valid_orders = [o for o in data['orders']
                       if o.get('financial_status') != 'voided' and not o.get('cancelled_at')]
        total_revenue = sum(o.get('current_total_price', 0) for o in valid_orders)

        # Add summary to returned data
        data['orders'] = {
            'items': data['orders'],
            'total_orders': len(valid_orders),
            'total_revenue': total_revenue,
            'all_orders_count': len(data['orders']),
        }

        log.info(f"Shopify sync complete: {len(valid_orders)} orders, ${total_revenue:.2f} revenue")

        return data

    async def _fetch_orders(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch ALL orders within date range with proper pagination.
        Uses Sydney timezone and fetches all order statuses.
        """
        try:
            start_str, end_str = self._get_sydney_date_range(start_date, end_date)

            log.info(f"Fetching orders from {start_str} to {end_str}")

            all_orders = []
            page = 1

            # First request
            orders = shopify.Order.find(
                status='any',  # Get ALL statuses: open, closed, cancelled, any
                created_at_min=start_str,
                created_at_max=end_str,
                limit=250  # Max allowed by Shopify
            )

            while orders:
                page_count = len(orders)
                log.info(f"Fetching orders page {page}: got {page_count} orders")

                for order in orders:
                    # Get current_total_price (actual revenue after refunds)
                    # Falls back to total_price if not available
                    current_price = float(order.current_total_price) if hasattr(order, 'current_total_price') and order.current_total_price else None
                    original_price = float(order.total_price) if order.total_price else 0
                    shipping_total = 0.0
                    if hasattr(order, 'total_shipping_price_set') and order.total_shipping_price_set:
                        try:
                            shipping_total = float(order.total_shipping_price_set.shop_money.amount)
                        except Exception:
                            shipping_total = 0.0
                    elif hasattr(order, 'total_shipping_price') and order.total_shipping_price:
                        shipping_total = float(order.total_shipping_price)
                    elif hasattr(order, 'shipping_lines') and order.shipping_lines:
                        try:
                            shipping_total = sum(float(line.price) for line in order.shipping_lines if line.price)
                        except Exception:
                            shipping_total = 0.0

                    order_data = {
                        "id": order.id,
                        "order_number": order.order_number,
                        "email": order.email,
                        "total_price": original_price,  # Original order total
                        "current_total_price": current_price if current_price is not None else original_price,  # After refunds
                        "subtotal_price": float(order.subtotal_price) if order.subtotal_price else 0,
                        "current_subtotal_price": float(order.current_subtotal_price) if hasattr(order, 'current_subtotal_price') and order.current_subtotal_price else None,
                        "total_tax": float(order.total_tax) if order.total_tax else 0,
                        "total_discounts": float(order.total_discounts) if order.total_discounts else 0,
                        "total_shipping": shipping_total,
                        "currency": order.currency,
                        "financial_status": order.financial_status,
                        "fulfillment_status": order.fulfillment_status,
                        "created_at": order.created_at,
                        "updated_at": order.updated_at,
                        "processed_at": order.processed_at,
                        "cancelled_at": getattr(order, 'cancelled_at', None),
                        "cancel_reason": getattr(order, 'cancel_reason', None),
                        "customer_id": order.customer.id if order.customer else None,
                        "line_items_count": len(order.line_items) if order.line_items else 0,
                        "line_items": self._extract_line_items(order.line_items) if order.line_items else [],
                        "source_name": order.source_name,
                        "referring_site": order.referring_site,
                        "landing_site": order.landing_site,
                        "tags": order.tags,
                        "note": order.note,
                        "gateway": getattr(order, 'gateway', None),
                    }
                    all_orders.append(order_data)

                # Check for more pages using cursor-based pagination
                if orders.has_next_page():
                    orders = orders.next_page()
                    page += 1
                else:
                    break

            # Calculate and log summary
            # Use current_total_price for accurate revenue (after refunds)
            # Exclude voided and cancelled orders from revenue calculation
            valid_orders = [o for o in all_orders
                          if o['financial_status'] != 'voided' and not o['cancelled_at']]
            total_revenue = sum(o['current_total_price'] for o in valid_orders)

            paid_orders = [o for o in valid_orders if o['financial_status'] in ('paid', 'partially_paid', 'authorized')]
            pending_orders = [o for o in valid_orders if o['financial_status'] == 'pending']
            refunded_orders = [o for o in valid_orders if o['financial_status'] in ('refunded', 'partially_refunded')]
            voided_cancelled = [o for o in all_orders if o['financial_status'] == 'voided' or o['cancelled_at']]

            log.info(f"=" * 50)
            log.info(f"SHOPIFY ORDER SYNC SUMMARY")
            log.info(f"=" * 50)
            log.info(f"Date range: {start_str} to {end_str}")
            log.info(f"Total orders fetched: {len(all_orders)}")
            log.info(f"Valid orders (excl. voided/cancelled): {len(valid_orders)}")
            log.info(f"Total revenue (current_total_price): ${total_revenue:.2f}")
            log.info(f"  - Paid/Authorized: {len(paid_orders)} orders")
            log.info(f"  - Pending: {len(pending_orders)} orders")
            log.info(f"  - Refunded/Partially: {len(refunded_orders)} orders")
            log.info(f"  - Voided/Cancelled (excluded): {len(voided_cancelled)} orders")
            log.info(f"Pages fetched: {page}")
            log.info(f"=" * 50)

            return all_orders

        except Exception as e:
            log.error(f"Error fetching Shopify orders: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            return []

    def _extract_line_items(self, line_items) -> List[Dict]:
        """Extract line item details from order"""
        items = []
        for item in line_items:
            items.append({
                "id": item.id,
                "title": item.title,
                "quantity": item.quantity,
                "price": float(item.price) if item.price else 0,
                "total_discount": float(item.total_discount) if getattr(item, 'total_discount', None) else 0,
                "sku": item.sku,
                "variant_id": item.variant_id,
                "product_id": item.product_id,
                "vendor": item.vendor,
            })
        return items

    async def _fetch_customers(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch customer data with pagination"""
        try:
            start_str, end_str = self._get_sydney_date_range(start_date, end_date)

            all_customers = []
            page = 1

            customers = shopify.Customer.find(
                created_at_min=start_str,
                created_at_max=end_str,
                limit=250
            )

            while customers:
                log.info(f"Fetching customers page {page}: got {len(customers)} customers")

                for customer in customers:
                    all_customers.append({
                        "id": customer.id,
                        "email": customer.email,
                        "first_name": customer.first_name,
                        "last_name": customer.last_name,
                        "orders_count": customer.orders_count,
                        "total_spent": float(customer.total_spent) if customer.total_spent else 0,
                        "created_at": customer.created_at,
                        "updated_at": customer.updated_at,
                        "state": customer.state,
                        "accepts_marketing": customer.accepts_marketing,
                    })

                if customers.has_next_page():
                    customers = customers.next_page()
                    page += 1
                else:
                    break

            log.info(f"Fetched {len(all_customers)} total customers from Shopify")
            return all_customers

        except Exception as e:
            log.error(f"Error fetching Shopify customers: {str(e)}")
            return []

    async def _fetch_products(self) -> List[Dict]:
        """Fetch product catalog with pagination"""
        try:
            all_products = []
            page = 1

            products = shopify.Product.find(limit=250)

            while products:
                log.info(f"Fetching products page {page}: got {len(products)} products")

                for product in products:
                    all_products.append({
                        "id": product.id,
                        "title": product.title,
                        "product_type": product.product_type,
                        "vendor": product.vendor,
                        "tags": product.tags,
                        "status": product.status,
                        "created_at": product.created_at,
                        "updated_at": product.updated_at,
                        "variants_count": len(product.variants) if product.variants else 0,
                    })

                if products.has_next_page():
                    products = products.next_page()
                    page += 1
                else:
                    break

            log.info(f"Fetched {len(all_products)} total products from Shopify")
            return all_products

        except Exception as e:
            log.error(f"Error fetching Shopify products: {str(e)}")
            return []

    async def _fetch_abandoned_checkouts(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch abandoned checkout data with pagination"""
        try:
            start_str, end_str = self._get_sydney_date_range(start_date, end_date)

            all_checkouts = []
            page = 1

            checkouts = shopify.Checkout.find(
                created_at_min=start_str,
                created_at_max=end_str,
                limit=250
            )

            while checkouts:
                log.info(f"Fetching abandoned checkouts page {page}: got {len(checkouts)} checkouts")

                for checkout in checkouts:
                    all_checkouts.append({
                        "id": checkout.id,
                        "token": checkout.token,
                        "email": checkout.email,
                        "total_price": float(checkout.total_price) if checkout.total_price else 0,
                        "created_at": checkout.created_at,
                        "updated_at": checkout.updated_at,
                        "completed_at": checkout.completed_at,
                        "abandoned_checkout_url": checkout.abandoned_checkout_url,
                    })

                if checkouts.has_next_page():
                    checkouts = checkouts.next_page()
                    page += 1
                else:
                    break

            log.info(f"Fetched {len(all_checkouts)} total abandoned checkouts from Shopify")
            return all_checkouts

        except Exception as e:
            log.error(f"Error fetching abandoned checkouts: {str(e)}")
            return []

    async def _fetch_shop_info(self) -> Dict:
        """Fetch shop information"""
        try:
            shop = shopify.Shop.current()
            return {
                "name": shop.name,
                "domain": shop.domain,
                "email": shop.email,
                "currency": shop.currency,
                "timezone": shop.timezone,
                "iana_timezone": shop.iana_timezone,
                "plan_name": shop.plan_name,
            }
        except Exception as e:
            log.error(f"Error fetching shop info: {str(e)}")
            return {}

    async def _fetch_products_full(self) -> List[Dict]:
        """
        Fetch full product catalog with complete variant details including SKU.
        Used for backfill to get all product data.
        """
        try:
            all_products = []
            page = 1

            products = shopify.Product.find(limit=250)

            while products:
                log.info(f"Fetching full products page {page}: got {len(products)} products")

                for product in products:
                    # Extract full variant details
                    variants_list = []
                    if product.variants:
                        for variant in product.variants:
                            variants_list.append({
                                'id': variant.id,
                                'title': variant.title,
                                'sku': variant.sku,
                                'price': float(variant.price) if variant.price else 0,
                                'compare_at_price': float(variant.compare_at_price) if variant.compare_at_price else None,
                                'inventory_quantity': variant.inventory_quantity,
                                'inventory_item_id': variant.inventory_item_id,
                                'weight': variant.weight,
                                'weight_unit': variant.weight_unit,
                                'barcode': variant.barcode,
                            })

                    # Extract images
                    images_list = []
                    if product.images:
                        for image in product.images:
                            images_list.append({
                                'id': image.id,
                                'src': image.src,
                                'alt': getattr(image, 'alt', None),
                            })

                    all_products.append({
                        'id': product.id,
                        'title': product.title,
                        'handle': product.handle,
                        'body_html': product.body_html,
                        'product_type': product.product_type,
                        'vendor': product.vendor,
                        'tags': product.tags,
                        'status': product.status,
                        'created_at': product.created_at,
                        'updated_at': product.updated_at,
                        'published_at': product.published_at,
                        'variants': variants_list,
                        'images': images_list,
                        'image': {'src': product.image.src} if product.image else None,
                    })

                if products.has_next_page():
                    products = products.next_page()
                    page += 1
                else:
                    break

            log.info(f"Fetched {len(all_products)} total products with full variant details")
            return all_products

        except Exception as e:
            log.error(f"Error fetching full Shopify products: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            return []

    async def _fetch_all_customers(self) -> List[Dict]:
        """
        Fetch ALL customers (not date-filtered) for backfill.
        Uses pagination to get complete customer list.
        """
        try:
            all_customers = []
            page = 1

            customers = shopify.Customer.find(limit=250)

            while customers:
                log.info(f"Fetching all customers page {page}: got {len(customers)} customers")

                for customer in customers:
                    default_address = None
                    addr = getattr(customer, 'default_address', None)
                    if addr:
                        default_address = {
                            'city': getattr(addr, 'city', None),
                            'province': getattr(addr, 'province', None),
                            'country': getattr(addr, 'country', None),
                            'zip': getattr(addr, 'zip', None),
                        }

                    all_customers.append({
                        'id': customer.id,
                        'email': getattr(customer, 'email', None),
                        'first_name': getattr(customer, 'first_name', None),
                        'last_name': getattr(customer, 'last_name', None),
                        'phone': getattr(customer, 'phone', None),
                        'orders_count': getattr(customer, 'orders_count', 0),
                        'total_spent': float(customer.total_spent) if getattr(customer, 'total_spent', None) else 0,
                        'created_at': getattr(customer, 'created_at', None),
                        'updated_at': getattr(customer, 'updated_at', None),
                        'state': getattr(customer, 'state', None),
                        'verified_email': getattr(customer, 'verified_email', False),
                        'accepts_marketing': getattr(customer, 'accepts_marketing', False),
                        'marketing_opt_in_level': getattr(customer, 'marketing_opt_in_level', None),
                        'tags': getattr(customer, 'tags', None),
                        'default_address': default_address,
                    })

                if customers.has_next_page():
                    customers = customers.next_page()
                    page += 1
                else:
                    break

            log.info(f"Fetched {len(all_customers)} total customers from Shopify")
            return all_customers

        except Exception as e:
            log.error(f"Error fetching all Shopify customers: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            return []

    async def _fetch_refunds(self, order_ids: List[int] = None) -> List[Dict]:
        """
        Fetch refunds for orders.

        Args:
            order_ids: List of order IDs to fetch refunds for. If None, fetches from recent orders.
        """
        try:
            all_refunds = []

            if not order_ids:
                log.warning("No order IDs provided for refund fetch")
                return []

            log.info(f"Fetching refunds for {len(order_ids)} orders")

            for i, order_id in enumerate(order_ids):
                try:
                    refunds = shopify.Refund.find(order_id=order_id)

                    for refund in refunds:
                        refund_line_items = []
                        if refund.refund_line_items:
                            for item in refund.refund_line_items:
                                line_item = item.line_item if hasattr(item, 'line_item') else {}
                                refund_line_items.append({
                                    'line_item_id': item.line_item_id,
                                    'quantity': item.quantity,
                                    'subtotal': float(item.subtotal) if item.subtotal else 0,
                                    'total_tax': float(item.total_tax) if item.total_tax else 0,
                                    'sku': line_item.get('sku') if isinstance(line_item, dict) else getattr(line_item, 'sku', None),
                                    'product_id': line_item.get('product_id') if isinstance(line_item, dict) else getattr(line_item, 'product_id', None),
                                })

                        # Calculate total refunded
                        total = sum(item['subtotal'] + item['total_tax'] for item in refund_line_items)

                        all_refunds.append({
                            'id': refund.id,
                            'order_id': order_id,
                            'created_at': self._parse_datetime(refund.created_at),
                            'processed_at': self._parse_datetime(refund.processed_at),
                            'note': refund.note,
                            'refund_line_items': refund_line_items,
                            'total_refunded': total,
                        })

                    # Rate limiting: pause every 50 orders
                    if (i + 1) % 50 == 0:
                        log.info(f"Processed refunds for {i + 1}/{len(order_ids)} orders, pausing for rate limit...")
                        await asyncio.sleep(1)

                except Exception as e:
                    log.warning(f"Error fetching refunds for order {order_id}: {e}")
                    continue

            log.info(f"Fetched {len(all_refunds)} total refunds from Shopify")
            return all_refunds

        except Exception as e:
            log.error(f"Error fetching Shopify refunds: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            return []

    async def _fetch_inventory(self) -> List[Dict]:
        """
        Fetch current inventory levels for all products/variants.
        Returns a snapshot of current inventory.
        """
        try:
            all_inventory = []
            page = 1

            # First get all products with their variants
            products = shopify.Product.find(limit=250)
            all_variants = []

            while products:
                for product in products:
                    if product.variants:
                        for variant in product.variants:
                            all_variants.append({
                                'product_id': product.id,
                                'product_title': product.title,
                                'vendor': product.vendor,
                                'variant_id': variant.id,
                                'variant_title': variant.title,
                                'sku': variant.sku,
                                'inventory_item_id': variant.inventory_item_id,
                                'inventory_quantity': variant.inventory_quantity,
                                'inventory_policy': variant.inventory_policy,
                                'price': float(variant.price) if variant.price else 0,
                            })

                if products.has_next_page():
                    products = products.next_page()
                    page += 1
                else:
                    break

            log.info(f"Found {len(all_variants)} variants across {page} product pages")

            # Now fetch inventory items for cost data (in batches)
            inventory_item_ids = [v['inventory_item_id'] for v in all_variants if v['inventory_item_id']]

            # Create lookup for cost data
            cost_lookup = {}

            # Fetch in batches of 100 (Shopify limit)
            batch_size = 100
            for i in range(0, len(inventory_item_ids), batch_size):
                batch_ids = inventory_item_ids[i:i + batch_size]
                try:
                    inventory_items = shopify.InventoryItem.find(ids=','.join(str(id) for id in batch_ids))
                    for item in inventory_items:
                        cost_lookup[item.id] = float(item.cost) if item.cost else None

                    # Rate limit
                    if i > 0 and i % (batch_size * 5) == 0:
                        await asyncio.sleep(1)

                except Exception as e:
                    log.warning(f"Error fetching inventory items batch: {e}")
                    continue

            # Build final inventory list
            for variant in all_variants:
                inv_item_id = variant['inventory_item_id']
                all_inventory.append({
                    'inventory_item_id': inv_item_id,
                    'product_id': variant['product_id'],
                    'variant_id': variant['variant_id'],
                    'sku': variant['sku'],
                    'title': f"{variant['product_title']} - {variant['variant_title']}" if variant['variant_title'] != 'Default Title' else variant['product_title'],
                    'inventory_quantity': variant['inventory_quantity'],
                    'inventory_policy': variant['inventory_policy'],
                    'cost': cost_lookup.get(inv_item_id),
                    'vendor': variant['vendor'],
                })

            log.info(f"Fetched inventory for {len(all_inventory)} variants")
            return all_inventory

        except Exception as e:
            log.error(f"Error fetching Shopify inventory: {str(e)}")
            import traceback
            log.error(traceback.format_exc())
            return []

    async def fetch_backfill_data(self, days: int = 365) -> Dict[str, Any]:
        """
        Fetch full historical data for backfill.

        Args:
            days: Number of days to backfill orders (default 365)

        Returns:
            Dict with products, customers, orders, refunds, inventory
        """
        if not self.session:
            await self.connect()

        log.info(f"Starting Shopify backfill: {days} days of orders")

        end_date = datetime.now(SYDNEY_TZ)
        start_date = end_date - timedelta(days=days)

        # Fetch all data
        products = await self._fetch_products_full()
        customers = await self._fetch_all_customers()
        orders = await self._fetch_orders(start_date, end_date)

        # Get order IDs for refund fetch
        order_ids = [o['id'] for o in orders if o.get('financial_status') in ('refunded', 'partially_refunded')]
        refunds = await self._fetch_refunds(order_ids) if order_ids else []

        inventory = await self._fetch_inventory()

        return {
            'products': {'items': products},
            'customers': {'items': customers},
            'orders': {'items': orders},
            'refunds': {'items': refunds},
            'inventory': {'items': inventory},
            'summary': {
                'products_count': len(products),
                'customers_count': len(customers),
                'orders_count': len(orders),
                'refunds_count': len(refunds),
                'inventory_count': len(inventory),
                'date_range': f'{start_date.date()} to {end_date.date()}',
            }
        }
