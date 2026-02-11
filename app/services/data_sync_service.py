"""
Data Synchronization Service
Orchestrates data syncing from all sources and persists to database
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal
import re
import pytz
from dateutil import parser as date_parser
from app.connectors.shopify_connector import ShopifyConnector
from app.connectors.klaviyo_connector import KlaviyoConnector
from app.connectors.ga4_connector import GA4Connector
from app.connectors.google_ads_connector import GoogleAdsConnector
from app.connectors.merchant_center_connector import MerchantCenterConnector
from app.connectors.github_connector import GitHubConnector
from app.connectors.search_console_connector import SearchConsoleConnector
from app.connectors.google_sheets import GoogleSheetsConnector
from app.connectors.shippit_connector import ShippitConnector
from app.config import get_settings
from app.models.base import SessionLocal
from app.models.shopify import ShopifyOrder, ShopifyProduct, ShopifyCustomer, ShopifyRefund, ShopifyRefundLineItem, ShopifyInventory, ShopifyOrderItem
from app.models.shippit import ShippitOrder
from app.utils.url_parsing import parse_landing_site
from app.models.search_console_data import SearchConsoleQuery, SearchConsolePage, SearchConsoleSitemap
from app.models.ga4_data import (
    GA4TrafficSource, GA4PagePerformance, GA4LandingPage, GA4ProductPerformance,
    GA4Event, GA4DailyEcommerce, GA4DailySummary, GA4DeviceBreakdown,
    GA4GeoBreakdown, GA4UserType
)
from app.models.klaviyo_data import KlaviyoCampaign, KlaviyoFlow, KlaviyoFlowMessage, KlaviyoSegment
from app.models.google_ads_data import GoogleAdsCampaign, GoogleAdsAdGroup, GoogleAdsSearchTerm
from app.models.merchant_center_data import MerchantCenterProductStatus, MerchantCenterDisapproval, MerchantCenterAccountStatus
from app.models.analytics import DataSyncLog
from app.models.product_cost import ProductCost
from app.models.data_quality import DataSyncStatus
from app.services.validation_service import validation_service
from app.utils.logger import log
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, List

# Sydney timezone for Cass Brothers
SYDNEY_TZ = pytz.timezone('Australia/Sydney')
settings = get_settings()


@dataclass
class SyncResult:
    """Tracks sync operation results for logging"""
    source: str
    sync_type: str = "incremental"
    status: str = "success"  # success, failed, partial
    records_processed: int = 0
    records_created: int = 0
    records_updated: int = 0
    records_failed: int = 0
    failed_record_ids: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    error_details: Optional[dict] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    # Retry tracking
    retry_attempts: int = 1
    retry_delay_seconds: float = 0.0
    retry_errors: List[str] = field(default_factory=list)


@contextmanager
def track_sync(source: str, sync_type: str = "incremental"):
    """
    Context manager to track sync timing and capture results.

    Usage:
        with track_sync("shopify") as result:
            # do sync work
            result.records_created = 50
            result.records_updated = 10
    """
    result = SyncResult(source=source, sync_type=sync_type)
    result.started_at = datetime.utcnow()
    start_time = time.time()

    try:
        yield result
    except Exception as e:
        result.status = "failed"
        result.error_message = str(e)
        result.error_details = {"exception_type": type(e).__name__}
        log.error(f"Sync failed for {source}: {e}")
        raise
    finally:
        result.completed_at = datetime.utcnow()
        result.duration_seconds = time.time() - start_time

        # Determine final status
        if result.status != "failed":
            if result.records_failed > 0 and result.records_created > 0:
                result.status = "partial"
            elif result.records_failed > 0 and result.records_created == 0:
                result.status = "failed"

        # Update the per-source data_sync_status table for freshness tracking
        try:
            update_data_sync_status(result)
        except Exception:
            pass  # Never let status tracking break the sync


def _persist_sync_log(result: SyncResult) -> Optional[int]:
    """
    Persist sync result to database.
    Returns the log ID or None if failed.

    Note: This creates an initial log entry. Call _update_sync_log() after
    save operations complete to record final counts and status.
    """
    db = SessionLocal()
    try:
        # Build error_details with retry stats and failed IDs
        error_details = result.error_details.copy() if result.error_details else {}

        if result.failed_record_ids:
            error_details["failed_record_ids"] = result.failed_record_ids[:100]

        # Include retry stats if there were retries
        # retries = number of extra attempts due to transient failures (0 = first try succeeded)
        if result.retry_attempts > 1:
            error_details["retry_stats"] = {
                "retries": result.retry_attempts - 1,
                "total_delay_seconds": round(result.retry_delay_seconds, 2),
                "errors": result.retry_errors[:5]  # Cap at 5 errors
            }

        sync_log = DataSyncLog(
            source=result.source,
            sync_type=result.sync_type,
            status=result.status,
            records_processed=result.records_processed,
            records_created=result.records_created,
            records_updated=result.records_updated,
            records_failed=result.records_failed,
            error_message=result.error_message,
            error_details=error_details if error_details else None,
            duration_seconds=result.duration_seconds,
            started_at=result.started_at,
            completed_at=result.completed_at
        )
        db.add(sync_log)
        db.commit()
        log.debug(f"Sync log created: {result.source} | id={sync_log.id}")
        return sync_log.id
    except Exception as e:
        db.rollback()
        log.error(f"Failed to persist sync log for {result.source}: {e}")
        return None
    finally:
        db.close()


def _update_sync_log(sync_log_id: int, result: SyncResult) -> bool:
    """
    Update an existing sync log with final counts and status.

    Called after save operations complete to record actual results.
    Returns True if update succeeded, False otherwise.
    """
    if not sync_log_id:
        return False

    db = SessionLocal()
    try:
        sync_log = db.query(DataSyncLog).filter(DataSyncLog.id == sync_log_id).first()
        if not sync_log:
            log.warning(f"Sync log {sync_log_id} not found for update")
            return False

        # Update counts
        sync_log.records_processed = result.records_processed
        sync_log.records_created = result.records_created
        sync_log.records_updated = result.records_updated
        sync_log.records_failed = result.records_failed

        # Update status based on validation/save results
        has_failures = result.records_failed > 0
        has_successes = result.records_created > 0 or result.records_updated > 0

        if has_failures and has_successes:
            sync_log.status = "partial"
        elif has_failures and not has_successes:
            sync_log.status = "failed"
        else:
            sync_log.status = result.status

        # Update timing (in case save took a while)
        sync_log.completed_at = result.completed_at or datetime.utcnow()
        sync_log.duration_seconds = result.duration_seconds

        # Update error details with failed record IDs
        if result.failed_record_ids:
            existing_details = sync_log.error_details or {}
            existing_details["failed_record_ids"] = result.failed_record_ids[:100]
            sync_log.error_details = existing_details

        db.commit()
        log.info(
            f"Sync logged: {result.source} | {sync_log.status} | "
            f"created={result.records_created} updated={result.records_updated} "
            f"failed={result.records_failed} | {result.duration_seconds:.2f}s"
        )
        return True
    except Exception as e:
        db.rollback()
        log.error(f"Failed to update sync log {sync_log_id}: {e}")
        return False
    finally:
        db.close()


# Source type mapping for data_sync_status
_SOURCE_TYPES = {
    'shopify': 'ecommerce',
    'shopify_inventory': 'ecommerce',
    'klaviyo': 'email',
    'ga4': 'analytics',
    'google_ads': 'advertising',
    'merchant_center': 'feed',
    'search_console': 'seo',
    'github': 'code',
    'cost_sheet': 'feed',
}


def update_data_sync_status(result: SyncResult) -> None:
    """
    Upsert the data_sync_status table after each sync.

    This table gives a single-row-per-source view of freshness,
    health, and error state — consumed by the data-quality dashboard
    and the stale-data warning banner.
    """
    db = SessionLocal()
    try:
        status = db.query(DataSyncStatus).filter(
            DataSyncStatus.source_name == result.source
        ).first()

        if not status:
            status = DataSyncStatus(
                source_name=result.source,
                source_type=_SOURCE_TYPES.get(result.source, 'other'),
            )
            db.add(status)

        status.last_sync_attempt = result.started_at or datetime.utcnow()
        status.sync_duration_seconds = result.duration_seconds
        status.records_synced = result.records_created + result.records_updated
        status.records_failed = result.records_failed

        if result.status in ('success', 'partial'):
            status.last_successful_sync = result.completed_at or datetime.utcnow()
            status.sync_status = result.status
            status.error_count = 0
            status.first_error_at = None
            status.last_error = None
            status.is_healthy = True
            status.health_score = 100 if result.status == 'success' else 80
            status.health_issues = None

            # Estimate data freshness from completed_at
            status.latest_data_timestamp = result.completed_at or datetime.utcnow()
            status.data_lag_hours = 0.0
        else:
            status.sync_status = 'failed'
            status.last_error = result.error_message
            status.error_count = (status.error_count or 0) + 1
            if not status.first_error_at:
                status.first_error_at = datetime.utcnow()

            # Degrade health based on consecutive failures
            if status.error_count >= 5:
                status.health_score = 0
                status.is_healthy = False
            elif status.error_count >= 3:
                status.health_score = 30
                status.is_healthy = False
            else:
                status.health_score = max(0, 100 - status.error_count * 20)
                status.is_healthy = status.health_score >= 50

            # Calculate data lag if we have a last successful sync
            if status.last_successful_sync:
                lag = datetime.utcnow() - status.last_successful_sync
                status.data_lag_hours = round(lag.total_seconds() / 3600, 1)

            status.health_issues = [result.error_message] if result.error_message else None

        status.updated_at = datetime.utcnow()
        db.commit()
    except Exception as e:
        db.rollback()
        log.error(f"Failed to update data_sync_status for {result.source}: {e}")
    finally:
        db.close()


class DataSyncService:
    """
    Manages synchronization of data from all sources
    """

    def __init__(self):
        self.shopify = ShopifyConnector()
        self.klaviyo = KlaviyoConnector()
        self.ga4 = GA4Connector()
        self.google_ads = GoogleAdsConnector()
        self.merchant_center = MerchantCenterConnector()
        self.github = GitHubConnector()
        self.search_console = SearchConsoleConnector()
        self.shippit = ShippitConnector() if settings.shippit_api_key else None
        self.google_sheets = None

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

    def _get_sydney_date_range(self, days: int) -> tuple:
        """
        Get date range in Sydney timezone.
        days=0: just today
        days=1: today and yesterday
        days=7: last 7 days including today
        """
        now_sydney = datetime.now(SYDNEY_TZ)

        # End of today in Sydney
        end_date = now_sydney.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Start of the range (days ago at midnight)
        start_date = (now_sydney - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        log.info(f"Date range (Sydney): {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")

        return start_date, end_date

    async def sync_all(self, days: int = 30) -> Dict:
        """
        Sync data from all sources.
        Each individual sync logs its own result to DataSyncLog.
        """
        log.info(f"Starting full data sync for last {days} days")

        start_date, end_date = self._get_sydney_date_range(days)

        # Use the logged sync methods instead of raw connector calls
        results = {
            'shopify': await self.sync_shopify(days=days),
            'klaviyo': await self.sync_klaviyo(days=days),
            'ga4': await self.sync_ga4(days=days),
            'google_ads': await self.sync_google_ads(days=days),
            'merchant_center': await self.sync_merchant_center(),
        }

        if self.shippit:
            results['shippit'] = await self.sync_shippit(days=days)

        # Summary
        success_count = sum(1 for r in results.values() if r.get('success'))
        total_duration = sum(r.get('duration', 0) for r in results.values())

        summary = {
            'success': success_count == len(results),
            'sources_synced': success_count,
            'total_sources': len(results),
            'total_duration': total_duration,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'results': results,
            'sync_log_ids': {
                source: r.get('sync_log_id')
                for source, r in results.items()
                if r.get('sync_log_id')
            }
        }

        log.info(f"Data sync complete: {success_count}/{len(results)} sources successful")
        return summary

    async def sync_cost_sheet(self) -> Dict:
        """
        Sync NETT Master Sheet (Google Sheets) into product_costs table.
        """
        with track_sync("cost_sheet", "full") as sync_result:
            db = SessionLocal()
            try:
                self.google_sheets = GoogleSheetsConnector(
                    db=db,
                    credentials_path=settings.google_sheets_credentials_path,
                    sheet_id=settings.cost_sheet_id,
                    sheet_range=settings.cost_sheet_range,
                    tab_prefix=settings.cost_sheet_tab_prefix
                )

                result = await self.google_sheets.sync()

                if not result.get("success"):
                    sync_result.status = "failed"
                    sync_result.error_message = result.get("error", "Unknown error")
                    _persist_sync_log(sync_result)
                    return result

                sync_log_id = _persist_sync_log(sync_result)
                result["sync_log_id"] = sync_log_id

                sync_result.records_created = result.get("records_synced", 0)
                sync_result.records_processed = (
                    result.get("records_synced", 0) + result.get("records_skipped", 0)
                )
                sync_result.records_failed = result.get("errors", 0)

                _update_sync_log(sync_log_id, sync_result)

                result["duration"] = sync_result.duration_seconds
                return result
            finally:
                db.close()

    async def sync_shopify(self, days: int = 30, include_products: bool = True, save_to_db: bool = True) -> Dict:
        """
        Sync Shopify data and save to database

        Args:
            days: Number of days to sync (0=today only)
            include_products: If False, skip product fetch (much faster ~5s vs ~2min)
            save_to_db: If True, persist data to database
        """
        with track_sync("shopify", "incremental") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            result = await self.shopify.sync(start_date, end_date, include_products=include_products)

            # Capture retry stats from connector
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist the sync log first to get the ID for validation tracking
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                data = result.get('data', {})

                # Save orders
                save_result = self._save_shopify_orders(data, sync_log_id=sync_log_id)
                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']
                sync_result.failed_record_ids = save_result.get('failed_ids', [])

                result['orders_saved'] = save_result['created']
                result['orders_updated'] = save_result['updated']
                result['orders_failed'] = save_result['failed']
                result['validation_failures'] = save_result.get('validation_failures', 0)

                # Save products (if fetched)
                if include_products and data.get('products'):
                    products_result = self._save_shopify_products(data)
                    result['products_saved'] = products_result['created']
                    result['products_updated'] = products_result['updated']
                    sync_result.records_created += products_result['created']
                    sync_result.records_updated += products_result['updated']

                # Save customers (if fetched)
                if data.get('customers'):
                    customers_result = self._save_shopify_customers(data)
                    result['customers_saved'] = customers_result['created']
                    result['customers_updated'] = customers_result['updated']
                    sync_result.records_created += customers_result['created']
                    sync_result.records_updated += customers_result['updated']

                # Fetch and save refunds for refunded/partially_refunded orders
                if not data.get('refunds'):
                    orders_list = data.get('orders', {})
                    if isinstance(orders_list, dict):
                        orders_list = orders_list.get('items', [])
                    refund_order_ids = [
                        o['id'] for o in orders_list
                        if o.get('financial_status') in ('refunded', 'partially_refunded')
                    ]
                    if refund_order_ids:
                        log.info(f"Fetching refunds for {len(refund_order_ids)} refunded orders")
                        refund_items = await self.shopify._fetch_refunds(refund_order_ids)
                        if refund_items:
                            data['refunds'] = {'items': refund_items}

                if data.get('refunds'):
                    refunds_result = self._save_shopify_refunds(data)
                    result['refunds_saved'] = refunds_result['created']
                    result['refunds_updated'] = refunds_result['updated']
                    sync_result.records_created += refunds_result['created']
                    sync_result.records_updated += refunds_result['updated']

                # Legacy fields for backwards compatibility
                result['saved_to_db'] = result.get('orders_saved', 0)
                result['updated_in_db'] = result.get('orders_updated', 0)
                result['failed_to_save'] = result.get('orders_failed', 0)

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

            # Add duration to result for sync_all aggregation
            result['duration'] = sync_result.duration_seconds

        return result

    def _save_shopify_orders(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save Shopify orders to database with validation.

        Returns:
            Dict with keys: processed, created, updated, failed, failed_ids, validation_failures
        """
        result = {
            'processed': 0,
            'created': 0,
            'updated': 0,
            'failed': 0,
            'failed_ids': [],
            'validation_failures': 0
        }

        orders_data = data.get('orders', {})
        orders = orders_data.get('items', []) if isinstance(orders_data, dict) else orders_data
        if not orders:
            return result

        db = SessionLocal()

        try:
            # Build comprehensive SKU -> cost lookup (supports fuzzy matching)
            cost_rows = db.query(
                ProductCost.vendor_sku,
                ProductCost.description,
                ProductCost.nett_nett_cost_inc_gst,
                ProductCost.has_active_special,
                ProductCost.special_cost_inc_gst,
            ).filter(
                ProductCost.nett_nett_cost_inc_gst.isnot(None),
            ).all()

            _cost_exact = {}       # exact vendor_sku -> cost
            _cost_lower = {}       # lowercase vendor_sku -> cost
            _cost_desc_prefix = {} # first token of description -> cost (Oliveri)

            for _sku, _desc, _nett, _has_sp, _sp_cost in cost_rows:
                if not _sku or not _nett:
                    continue
                _active = _sp_cost if _has_sp and _sp_cost else _nett
                _cost_exact[_sku] = _active
                _cost_lower[_sku.lower()] = _active
                if _desc:
                    _tok = _desc.split()[0] if _desc.strip() else None
                    if _tok and _tok != _sku and re.search(r'[A-Za-z]', _tok):
                        _cost_desc_prefix[_tok] = _active
                        _cost_desc_prefix[_tok.upper()] = _active

            def _lookup_cost(sku):
                """Fuzzy SKU -> cost lookup with 4 strategies."""
                if sku in _cost_exact:
                    return _cost_exact[sku]
                if sku.lower() in _cost_lower:
                    return _cost_lower[sku.lower()]
                if sku in _cost_desc_prefix:
                    return _cost_desc_prefix[sku]
                if sku.upper() in _cost_desc_prefix:
                    return _cost_desc_prefix[sku.upper()]
                base = re.sub(r'G\d.*$', '', sku)
                if base != sku:
                    if base in _cost_exact:
                        return _cost_exact[base]
                    if base.lower() in _cost_lower:
                        return _cost_lower[base.lower()]
                return None

            for order_data in orders:
                shopify_order_id = order_data.get('id')

                # Validate the order before processing
                validation_result = validation_service.validate_shopify_order(order_data)

                # Persist any validation issues (errors and warnings)
                if validation_result.all_issues:
                    result['validation_failures'] += validation_service.persist_validation_failures(
                        failures=validation_result.all_issues,
                        entity_type="order",
                        entity_id=shopify_order_id,
                        source="shopify",
                        sync_log_id=sync_log_id
                    )

                # Skip if blocking validation errors
                if validation_result.has_blocking_errors:
                    result['failed'] += 1
                    result['failed_ids'].append(str(shopify_order_id) if shopify_order_id else f"invalid_{result['failed']}")
                    log.warning(f"Order {shopify_order_id} failed validation: {[e.message for e in validation_result.errors]}")
                    continue

                result['processed'] += 1

                try:
                    # Check if order exists
                    existing = db.query(ShopifyOrder).filter(
                        ShopifyOrder.shopify_order_id == shopify_order_id
                    ).first()

                    if existing:
                        # Update existing order
                        existing.customer_id = order_data.get('customer_id')
                        existing.financial_status = order_data.get('financial_status')
                        existing.fulfillment_status = order_data.get('fulfillment_status')
                        existing.total_price = Decimal(str(order_data.get('total_price', 0)))
                        existing.current_total_price = Decimal(str(order_data.get('current_total_price', 0))) if order_data.get('current_total_price') else None
                        existing.current_subtotal_price = Decimal(str(order_data.get('current_subtotal_price', 0))) if order_data.get('current_subtotal_price') else None
                        existing.subtotal_price = Decimal(str(order_data.get('subtotal_price', 0)))
                        existing.total_shipping = Decimal(str(order_data.get('total_shipping', 0)))
                        existing.total_tax = Decimal(str(order_data.get('total_tax', 0)))
                        existing.total_discounts = Decimal(str(order_data.get('total_discounts', 0)))
                        existing.updated_at = datetime.utcnow()
                        result['updated'] += 1

                        # Upsert order items for this order
                        order_created_at = existing.created_at
                        financial_status = existing.financial_status
                        fulfillment_status = existing.fulfillment_status
                    else:
                        # Create new order
                        # Parse UTM and Google Ads params from landing URL
                        _utm = parse_landing_site(order_data.get('landing_site'))

                        order_created_at = datetime.fromisoformat(order_data['created_at'].replace('Z', '+00:00')) if order_data.get('created_at') else None
                        financial_status = order_data.get('financial_status')
                        fulfillment_status = order_data.get('fulfillment_status')

                        new_order = ShopifyOrder(
                            shopify_order_id=shopify_order_id,
                            order_number=order_data.get('order_number'),
                            customer_id=order_data.get('customer_id'),
                            customer_email=order_data.get('email'),
                            financial_status=financial_status,
                            fulfillment_status=fulfillment_status,
                            currency=order_data.get('currency', 'AUD'),
                            total_price=Decimal(str(order_data.get('total_price', 0))),
                            current_total_price=Decimal(str(order_data.get('current_total_price', 0))) if order_data.get('current_total_price') else None,
                            subtotal_price=Decimal(str(order_data.get('subtotal_price', 0))),
                            current_subtotal_price=Decimal(str(order_data.get('current_subtotal_price', 0))) if order_data.get('current_subtotal_price') else None,
                            total_tax=Decimal(str(order_data.get('total_tax', 0))),
                            total_discounts=Decimal(str(order_data.get('total_discounts', 0))),
                            total_shipping=Decimal(str(order_data.get('total_shipping', 0))),
                            line_items=order_data.get('line_items'),
                            discount_codes=order_data.get('discount_codes'),
                            landing_site=order_data.get('landing_site'),
                            referring_site=order_data.get('referring_site'),
                            source_name=order_data.get('source_name'),
                            utm_source=_utm.get("utm_source"),
                            utm_medium=_utm.get("utm_medium"),
                            utm_campaign=_utm.get("utm_campaign"),
                            utm_term=_utm.get("utm_term"),
                            utm_content=_utm.get("utm_content"),
                            gclid=_utm.get("gclid"),
                            gad_campaign_id=_utm.get("gad_campaign_id"),
                            tags=order_data.get('tags', '').split(', ') if order_data.get('tags') else None,
                            created_at=order_created_at,
                            updated_at=datetime.fromisoformat(order_data['updated_at'].replace('Z', '+00:00')) if order_data.get('updated_at') else None,
                            cancelled_at=datetime.fromisoformat(order_data['cancelled_at'].replace('Z', '+00:00')) if order_data.get('cancelled_at') else None,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_order)
                        result['created'] += 1

                    # ── Save normalized order items (enables COGS, product mix, P&L) ──
                    line_items = order_data.get('line_items', [])
                    if line_items:
                        # Delete existing items for this order (upsert pattern)
                        db.query(ShopifyOrderItem).filter(
                            ShopifyOrderItem.shopify_order_id == shopify_order_id
                        ).delete()

                        for item in line_items:
                            sku = item.get('sku')
                            # Look up COGS from product_costs table (fuzzy matching)
                            cost_per_item = _lookup_cost(sku.strip()) if sku else None

                            item_price = Decimal(str(item.get('price', 0)))
                            item_qty = int(item.get('quantity', 1))
                            item_discount = Decimal(str(item.get('total_discount', 0)))

                            order_item = ShopifyOrderItem(
                                shopify_order_id=shopify_order_id,
                                order_number=order_data.get('order_number'),
                                order_date=order_created_at,
                                line_item_id=item.get('id'),
                                shopify_product_id=item.get('product_id'),
                                shopify_variant_id=item.get('variant_id'),
                                sku=sku,
                                title=item.get('title'),
                                variant_title=item.get('variant_title'),
                                vendor=item.get('vendor'),
                                product_type=item.get('product_type'),
                                quantity=item_qty,
                                price=item_price,
                                total_price=item_price * item_qty,
                                total_discount=item_discount,
                                cost_per_item=cost_per_item,
                                financial_status=financial_status,
                                fulfillment_status=fulfillment_status,
                                synced_at=datetime.utcnow()
                            )
                            db.add(order_item)

                except Exception as e:
                    log.warning(f"Failed to save order {shopify_order_id}: {e}")
                    result['failed'] += 1
                    result['failed_ids'].append(str(shopify_order_id))
                    continue

            db.commit()
            log.info(f"Saved {result['created']} new, updated {result['updated']} Shopify orders (with order items)")
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shopify orders (batch failed): {e}")
            # Mark all remaining as failed
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_shopify_products(self, data: Dict) -> Dict:
        """
        Save Shopify products to database.

        Returns:
            Dict with keys: processed, created, updated, failed
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0}
        products_data = data.get('products', {})
        products = products_data.get('items', []) if isinstance(products_data, dict) else products_data
        if not products:
            return result

        db = SessionLocal()
        try:
            for product_data in products:
                shopify_product_id = product_data.get('id')
                if not shopify_product_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(ShopifyProduct).filter(
                        ShopifyProduct.shopify_product_id == shopify_product_id
                    ).first()

                    if existing:
                        # Update existing product
                        existing.title = product_data.get('title', existing.title)
                        existing.handle = product_data.get('handle', existing.handle)
                        existing.body_html = product_data.get('body_html')
                        existing.vendor = product_data.get('vendor')
                        existing.product_type = product_data.get('product_type')
                        existing.tags = product_data.get('tags', '').split(', ') if product_data.get('tags') else None
                        existing.status = product_data.get('status')
                        existing.variants = product_data.get('variants')
                        existing.images = product_data.get('images')
                        existing.featured_image = product_data.get('image', {}).get('src') if product_data.get('image') else None
                        existing.updated_at = datetime.fromisoformat(product_data['updated_at'].replace('Z', '+00:00')) if product_data.get('updated_at') else datetime.utcnow()
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        # Create new product
                        new_product = ShopifyProduct(
                            shopify_product_id=shopify_product_id,
                            handle=product_data.get('handle'),
                            title=product_data.get('title', 'Unknown'),
                            body_html=product_data.get('body_html'),
                            vendor=product_data.get('vendor'),
                            product_type=product_data.get('product_type'),
                            tags=product_data.get('tags', '').split(', ') if product_data.get('tags') else None,
                            status=product_data.get('status', 'active'),
                            variants=product_data.get('variants'),
                            images=product_data.get('images'),
                            featured_image=product_data.get('image', {}).get('src') if product_data.get('image') else None,
                            created_at=datetime.fromisoformat(product_data['created_at'].replace('Z', '+00:00')) if product_data.get('created_at') else None,
                            updated_at=datetime.fromisoformat(product_data['updated_at'].replace('Z', '+00:00')) if product_data.get('updated_at') else None,
                            published_at=datetime.fromisoformat(product_data['published_at'].replace('Z', '+00:00')) if product_data.get('published_at') else None,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_product)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save product {shopify_product_id}: {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            log.info(f"Saved {result['created']} new, updated {result['updated']} Shopify products")
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shopify products: {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_shopify_customers(self, data: Dict) -> Dict:
        """
        Save Shopify customers to database.

        Returns:
            Dict with keys: processed, created, updated, failed
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0}
        customers_data = data.get('customers', {})
        customers = customers_data.get('items', []) if isinstance(customers_data, dict) else customers_data
        if not customers:
            return result

        db = SessionLocal()
        try:
            for customer_data in customers:
                shopify_customer_id = customer_data.get('id')
                if not shopify_customer_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(ShopifyCustomer).filter(
                        ShopifyCustomer.shopify_customer_id == shopify_customer_id
                    ).first()

                    default_address = customer_data.get('default_address', {}) or {}

                    if existing:
                        # Update existing customer
                        existing.email = customer_data.get('email', existing.email)
                        existing.first_name = customer_data.get('first_name')
                        existing.last_name = customer_data.get('last_name')
                        existing.phone = customer_data.get('phone')
                        existing.orders_count = customer_data.get('orders_count', 0)
                        existing.total_spent = Decimal(str(customer_data.get('total_spent', 0)))
                        existing.state = customer_data.get('state')
                        existing.verified_email = customer_data.get('verified_email', False)
                        existing.accepts_marketing = customer_data.get('accepts_marketing', False)
                        existing.marketing_opt_in_level = customer_data.get('marketing_opt_in_level')
                        existing.tags = customer_data.get('tags', '').split(', ') if customer_data.get('tags') else None
                        existing.default_address_city = default_address.get('city')
                        existing.default_address_province = default_address.get('province')
                        existing.default_address_country = default_address.get('country')
                        existing.default_address_zip = default_address.get('zip')
                        existing.updated_at = datetime.fromisoformat(customer_data['updated_at'].replace('Z', '+00:00')) if customer_data.get('updated_at') else datetime.utcnow()
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        # Create new customer
                        new_customer = ShopifyCustomer(
                            shopify_customer_id=shopify_customer_id,
                            email=customer_data.get('email'),
                            first_name=customer_data.get('first_name'),
                            last_name=customer_data.get('last_name'),
                            phone=customer_data.get('phone'),
                            orders_count=customer_data.get('orders_count', 0),
                            total_spent=Decimal(str(customer_data.get('total_spent', 0))),
                            state=customer_data.get('state'),
                            verified_email=customer_data.get('verified_email', False),
                            accepts_marketing=customer_data.get('accepts_marketing', False),
                            marketing_opt_in_level=customer_data.get('marketing_opt_in_level'),
                            tags=customer_data.get('tags', '').split(', ') if customer_data.get('tags') else None,
                            default_address_city=default_address.get('city'),
                            default_address_province=default_address.get('province'),
                            default_address_country=default_address.get('country'),
                            default_address_zip=default_address.get('zip'),
                            created_at=datetime.fromisoformat(customer_data['created_at'].replace('Z', '+00:00')) if customer_data.get('created_at') else None,
                            updated_at=datetime.fromisoformat(customer_data['updated_at'].replace('Z', '+00:00')) if customer_data.get('updated_at') else None,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_customer)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save customer {shopify_customer_id}: {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            log.info(f"Saved {result['created']} new, updated {result['updated']} Shopify customers")
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shopify customers: {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_shopify_refunds(self, data: Dict) -> Dict:
        """
        Save Shopify refunds to database.

        Returns:
            Dict with keys: processed, created, updated, failed
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0}
        refunds_data = data.get('refunds', {})
        refunds = refunds_data.get('items', []) if isinstance(refunds_data, dict) else refunds_data
        if not refunds:
            return result

        db = SessionLocal()
        try:
            for refund_data in refunds:
                shopify_refund_id = refund_data.get('id')
                if not shopify_refund_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(ShopifyRefund).filter(
                        ShopifyRefund.shopify_refund_id == shopify_refund_id
                    ).first()

                    # Calculate total refunded from line items if not provided
                    total_refunded = refund_data.get('total_refunded', 0)
                    if not total_refunded and refund_data.get('refund_line_items'):
                        total_refunded = sum(
                            Decimal(str(item.get('subtotal', 0)))
                            for item in refund_data.get('refund_line_items', [])
                        )

                    if existing:
                        # Update existing refund
                        existing.refund_line_items = refund_data.get('refund_line_items')
                        existing.total_refunded = Decimal(str(total_refunded))
                        existing.note = refund_data.get('note')
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        # Create new refund
                        new_refund = ShopifyRefund(
                            shopify_refund_id=shopify_refund_id,
                            shopify_order_id=refund_data.get('order_id'),
                            refund_line_items=refund_data.get('refund_line_items'),
                            total_refunded=Decimal(str(total_refunded)),
                            currency=refund_data.get('currency', 'AUD'),
                            note=refund_data.get('note'),
                            created_at=self._parse_datetime(refund_data.get('created_at')),
                            processed_at=self._parse_datetime(refund_data.get('processed_at')),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_refund)
                        result['created'] += 1

                    # Normalize refund line items
                    refund_items = refund_data.get('refund_line_items') or []
                    if refund_items:
                        db.query(ShopifyRefundLineItem).filter(
                            ShopifyRefundLineItem.shopify_refund_id == shopify_refund_id
                        ).delete()
                        for item in refund_items:
                            try:
                                new_item = ShopifyRefundLineItem(
                                    shopify_refund_id=shopify_refund_id,
                                    shopify_order_id=refund_data.get('order_id'),
                                    line_item_id=item.get('line_item_id'),
                                    shopify_product_id=item.get('product_id'),
                                    sku=item.get('sku'),
                                    quantity=int(item.get('quantity') or 0),
                                    subtotal=Decimal(str(item.get('subtotal', 0))),
                                    total_tax=Decimal(str(item.get('total_tax', 0))),
                                    created_at=self._parse_datetime(refund_data.get('created_at')),
                                    processed_at=self._parse_datetime(refund_data.get('processed_at')),
                                    synced_at=datetime.utcnow(),
                                )
                                db.add(new_item)
                            except Exception as item_e:
                                log.warning(f"Failed to save refund line item for refund {shopify_refund_id}: {item_e}")

                except Exception as e:
                    log.warning(f"Failed to save refund {shopify_refund_id}: {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            log.info(f"Saved {result['created']} new, updated {result['updated']} Shopify refunds")
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shopify refunds: {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_shopify_inventory(self, data: Dict) -> Dict:
        """
        Save Shopify inventory snapshot to database.

        Returns:
            Dict with keys: processed, created, updated, failed
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0}
        inventory_data = data.get('inventory', {})
        items = inventory_data.get('items', []) if isinstance(inventory_data, dict) else inventory_data
        if not items:
            return result

        db = SessionLocal()
        try:
            for item_data in items:
                inv_item_id = item_data.get('inventory_item_id')
                if not inv_item_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(ShopifyInventory).filter(
                        ShopifyInventory.shopify_inventory_item_id == inv_item_id
                    ).first()

                    if existing:
                        # Update existing inventory
                        existing.shopify_product_id = item_data.get('product_id')
                        existing.shopify_variant_id = item_data.get('variant_id')
                        existing.sku = item_data.get('sku')
                        existing.title = item_data.get('title')
                        existing.vendor = item_data.get('vendor')
                        existing.inventory_quantity = item_data.get('inventory_quantity', 0)
                        existing.inventory_policy = item_data.get('inventory_policy')
                        existing.cost = Decimal(str(item_data.get('cost'))) if item_data.get('cost') else None
                        existing.updated_at = datetime.utcnow()
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        # Create new inventory record
                        new_inv = ShopifyInventory(
                            shopify_inventory_item_id=inv_item_id,
                            shopify_product_id=item_data.get('product_id'),
                            shopify_variant_id=item_data.get('variant_id'),
                            sku=item_data.get('sku'),
                            title=item_data.get('title'),
                            vendor=item_data.get('vendor'),
                            inventory_quantity=item_data.get('inventory_quantity', 0),
                            inventory_policy=item_data.get('inventory_policy'),
                            cost=Decimal(str(item_data.get('cost'))) if item_data.get('cost') else None,
                            updated_at=datetime.utcnow(),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_inv)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save inventory {inv_item_id}: {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            log.info(f"Saved {result['created']} new, updated {result['updated']} Shopify inventory records")
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shopify inventory: {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    async def backfill_shopify(self, days: int = 365) -> Dict:
        """
        Backfill Shopify data: products, customers, orders, refunds, inventory.

        Args:
            days: Number of days to backfill orders (default 365)

        Returns:
            Dict with backfill results and DataSyncLog entry
        """
        backfill_start = time.time()

        log.info(f"Starting Shopify backfill: {days} days")

        # Create sync log for the entire backfill operation
        with track_sync("shopify", "backfill") as sync_result:
            try:
                # Fetch all data from Shopify
                data = await self.shopify.fetch_backfill_data(days=days)

                if not data:
                    sync_result.status = "failed"
                    sync_result.error_message = "No data returned from Shopify"
                    _persist_sync_log(sync_result)
                    return {'success': False, 'error': 'No data returned'}

                summary = data.get('summary', {})
                log.info(f"Fetched from Shopify: {summary}")

                results = {
                    'sync_type': 'backfill',
                    'days': days,
                    'fetch_summary': summary,
                    'save_results': {}
                }

                # Save products
                products_result = self._save_shopify_products(data)
                results['save_results']['products'] = products_result
                log.info(f"Products: {products_result}")

                # Save customers
                customers_result = self._save_shopify_customers(data)
                results['save_results']['customers'] = customers_result
                log.info(f"Customers: {customers_result}")

                # Save orders (this also creates order items)
                orders_result = self._save_shopify_orders(data)
                results['save_results']['orders'] = orders_result
                log.info(f"Orders: {orders_result}")

                # Save refunds
                refunds_result = self._save_shopify_refunds(data)
                results['save_results']['refunds'] = refunds_result
                log.info(f"Refunds: {refunds_result}")

                # Save inventory
                inventory_result = self._save_shopify_inventory(data)
                results['save_results']['inventory'] = inventory_result
                log.info(f"Inventory: {inventory_result}")

                # Calculate totals
                total_created = sum(r.get('created', 0) for r in results['save_results'].values())
                total_updated = sum(r.get('updated', 0) for r in results['save_results'].values())
                total_failed = sum(r.get('failed', 0) for r in results['save_results'].values())
                total_processed = sum(r.get('processed', 0) for r in results['save_results'].values())

                sync_result.records_created = total_created
                sync_result.records_updated = total_updated
                sync_result.records_failed = total_failed
                sync_result.records_processed = total_processed
                sync_result.duration_seconds = round(time.time() - backfill_start, 2)

                results['totals'] = {
                    'created': total_created,
                    'updated': total_updated,
                    'failed': total_failed,
                    'processed': total_processed,
                }
                results['duration_seconds'] = sync_result.duration_seconds
                results['success'] = True

                sync_log_id = _persist_sync_log(sync_result)
                results['sync_log_id'] = sync_log_id

                log.info(f"Shopify backfill complete: {total_created} created, {total_updated} updated in {sync_result.duration_seconds}s")

                return results

            except Exception as e:
                import traceback
                error_msg = f"Shopify backfill failed: {str(e)}"
                log.error(error_msg)
                log.error(traceback.format_exc())

                sync_result.status = "failed"
                sync_result.error_message = error_msg
                sync_result.duration_seconds = round(time.time() - backfill_start, 2)
                _persist_sync_log(sync_result)

                return {
                    'success': False,
                    'error': error_msg,
                    'sync_type': 'backfill',
                    'duration_seconds': sync_result.duration_seconds,
                }

    async def sync_shopify_inventory(self) -> Dict:
        """
        Sync current inventory snapshot from Shopify.

        Returns:
            Dict with inventory sync results
        """
        with track_sync("shopify_inventory", "snapshot") as sync_result:
            try:
                # Connect to Shopify
                await self.shopify.connect()

                # Fetch inventory
                inventory = await self.shopify._fetch_inventory()

                if not inventory:
                    sync_result.status = "failed"
                    sync_result.error_message = "No inventory data returned"
                    _persist_sync_log(sync_result)
                    return {'success': False, 'error': 'No inventory data'}

                # Save to database
                data = {'inventory': {'items': inventory}}
                save_result = self._save_shopify_inventory(data)

                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']

                sync_log_id = _persist_sync_log(sync_result)

                return {
                    'success': True,
                    'inventory_count': len(inventory),
                    'created': save_result['created'],
                    'updated': save_result['updated'],
                    'sync_log_id': sync_log_id,
                }

            except Exception as e:
                import traceback
                error_msg = f"Inventory sync failed: {str(e)}"
                log.error(error_msg)
                log.error(traceback.format_exc())

                sync_result.status = "failed"
                sync_result.error_message = error_msg
                _persist_sync_log(sync_result)

                return {'success': False, 'error': error_msg}

    async def sync_klaviyo(self, days: int = 30, save_to_db: bool = True) -> Dict:
        """Sync Klaviyo data and save to database"""
        with track_sync("klaviyo", "incremental") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            result = await self.klaviyo.sync(start_date, end_date)

            # Capture retry stats from connector
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist sync log first to get ID for validation tracking
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                save_result = self._save_klaviyo_data(result.get('data', {}), sync_log_id=sync_log_id)
                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']

                result['saved_to_db'] = save_result['created']
                result['updated_in_db'] = save_result['updated']
                result['validation_failures'] = save_result.get('validation_failures', 0)

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

            # Add duration to result for sync_all aggregation
            result['duration'] = sync_result.duration_seconds

        return result

    async def sync_ga4(self, days: int = 30, save_to_db: bool = True) -> Dict:
        """Sync GA4 data and save to database"""
        with track_sync("ga4", "incremental") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            result = await self.ga4.sync(start_date, end_date)

            # Capture retry stats from connector
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist sync log first to get ID for validation tracking
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                save_result = self._save_ga4_data(result.get('data', {}), sync_log_id=sync_log_id)
                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']

                result['saved_to_db'] = save_result['created']
                result['updated_in_db'] = save_result['updated']
                result['validation_failures'] = save_result.get('validation_failures', 0)

                # Add detailed save counts
                result['traffic_overview_saved'] = save_result.get('traffic_overview_saved', 0)
                result['traffic_sources_saved'] = save_result.get('traffic_sources_saved', 0)
                result['pages_saved'] = save_result.get('pages_saved', 0)
                result['landing_pages_saved'] = save_result.get('landing_pages_saved', 0)
                result['products_saved'] = save_result.get('products_saved', 0)
                result['events_saved'] = save_result.get('events_saved', 0)
                result['ecommerce_saved'] = save_result.get('ecommerce_saved', 0)

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

            # Add duration to result for sync_all aggregation
            result['duration'] = sync_result.duration_seconds

        return result

    async def sync_google_ads(self, days: int = 30, save_to_db: bool = True) -> Dict:
        """Sync Google Ads data and save to database"""
        with track_sync("google_ads", "incremental") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            result = await self.google_ads.sync(start_date, end_date)

            # Capture retry stats from connector
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist sync log first to get ID
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                data = result.get('data', {})
                # Pass the end_date as the reference date for aggregated data
                save_result = self._save_google_ads_data(data, reference_date=end_date.date(), sync_log_id=sync_log_id)
                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']

                result['campaigns_saved'] = save_result.get('campaigns_created', 0)
                result['campaigns_updated'] = save_result.get('campaigns_updated', 0)
                result['ad_groups_saved'] = save_result.get('ad_groups_created', 0)
                result['ad_groups_updated'] = save_result.get('ad_groups_updated', 0)
                result['search_terms_saved'] = save_result.get('search_terms_created', 0)
                result['search_terms_updated'] = save_result.get('search_terms_updated', 0)
                result['saved_to_db'] = save_result['created']
                result['updated_in_db'] = save_result['updated']

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

            # Add duration to result for sync_all aggregation
            result['duration'] = sync_result.duration_seconds

        return result

    async def sync_merchant_center(self, quick: bool = False, save_to_db: bool = True) -> Dict:
        """
        Sync Google Merchant Center data and save to database

        Args:
            quick: If True, skip full product list (faster for chat queries)
            save_to_db: If True, persist data to database
        """
        with track_sync("merchant_center", "quick" if quick else "full") as sync_result:
            start_date, end_date = self._get_sydney_date_range(0)
            if quick:
                # Use quick fetch that skips full product list
                data = await self.merchant_center.fetch_products_quick()
                result = {
                    "success": True,
                    "source": "Google Merchant Center",
                    "data": data,
                    "quick_mode": True
                }
            else:
                result = await self.merchant_center.sync(start_date, end_date)

            # Capture retry stats from connector (when available)
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist sync log first to get ID
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                data = result.get('data', {})
                # Pass today's date as snapshot date
                save_result = self._save_merchant_center_data(data, snapshot_date=end_date.date())
                sync_result.records_created = save_result['created']
                sync_result.records_updated = save_result['updated']
                sync_result.records_failed = save_result['failed']
                sync_result.records_processed = save_result['processed']

                result['statuses_saved'] = save_result.get('statuses_created', 0)
                result['statuses_updated'] = save_result.get('statuses_updated', 0)
                result['disapprovals_saved'] = save_result.get('disapprovals_created', 0)
                result['disapprovals_updated'] = save_result.get('disapprovals_updated', 0)
                result['account_status_saved'] = save_result.get('account_status_saved', False)
                result['saved_to_db'] = save_result['created']
                result['updated_in_db'] = save_result['updated']

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

            # Add duration to result for sync_all aggregation
            result['duration'] = sync_result.duration_seconds

        return result

    async def sync_github(self, days: int = 7, quick: bool = False) -> Dict:
        """
        Sync GitHub repository data

        Args:
            days: Number of days of commit history to fetch
            quick: If True, skip file contents (faster for chat queries)
        """
        with track_sync("github", "quick" if quick else "full") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            if quick:
                data = await self.github.fetch_quick()
                result = {
                    "success": True,
                    "source": "GitHub",
                    "data": data,
                    "quick_mode": True
                }
            else:
                result = await self.github.sync(start_date, end_date)

            # Capture retry stats from connector (when available)
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
            else:
                data = result.get('data', {})
                commits = data.get('commits', [])
                sync_result.records_processed = len(commits) if isinstance(commits, list) else 0

            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

        return result

    async def sync_search_console(self, days: int = 480, quick: bool = False, save_to_db: bool = True) -> Dict:
        """
        Sync Google Search Console data and save to database

        Args:
            days: Number of days to sync (max 480 = 16 months)
            quick: If True, only fetch last 7 days summary
            save_to_db: If True, persist data to database
        """
        with track_sync("search_console", "quick" if quick else "full") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)
            if quick:
                data = await self.search_console.fetch_quick()
                result = {
                    "success": True,
                    "source": "Google Search Console",
                    "data": data,
                    "quick_mode": True
                }
                sync_result.records_processed = len(data.get('queries', [])) if isinstance(data.get('queries'), list) else 0
                sync_log_id = _persist_sync_log(sync_result)
                result['sync_log_id'] = sync_log_id
                return result

            result = await self.search_console.sync(start_date, end_date)

            # Capture retry stats from connector
            # retries = extra attempts beyond the first (0 = succeeded first try)
            retry_stats = result.get('retry_stats', {})
            sync_result.retry_attempts = retry_stats.get('retries', 0) + 1  # Convert to total attempts
            sync_result.retry_delay_seconds = retry_stats.get('total_delay_seconds', 0)
            sync_result.retry_errors = retry_stats.get('errors', [])

            if not result.get('success'):
                sync_result.status = "failed"
                sync_result.error_message = result.get('error', 'Unknown error')
                _persist_sync_log(sync_result)
                return result

            # Persist sync log first to get ID for validation tracking
            sync_log_id = _persist_sync_log(sync_result)
            result['sync_log_id'] = sync_log_id

            if save_to_db:
                data = result.get('data', {})

                # Save queries
                query_save_result = self._save_search_queries(data, sync_log_id=sync_log_id)

                # Save pages (was previously missing — root cause of pages lag)
                page_save_result = self._save_search_pages(data, sync_log_id=sync_log_id)

                # Save sitemaps
                sitemap_save_result = self._save_search_sitemaps(data, sync_log_id=sync_log_id)

                total_created = query_save_result['created'] + page_save_result['created'] + sitemap_save_result['created']
                total_updated = query_save_result['updated'] + page_save_result['updated'] + sitemap_save_result['updated']
                total_failed = query_save_result['failed'] + page_save_result['failed'] + sitemap_save_result['failed']
                total_processed = query_save_result['processed'] + page_save_result['processed'] + sitemap_save_result['processed']

                sync_result.records_created = total_created
                sync_result.records_updated = total_updated
                sync_result.records_failed = total_failed
                sync_result.records_processed = total_processed

                result['queries_saved'] = query_save_result['created'] + query_save_result['updated']
                result['pages_saved'] = page_save_result['created'] + page_save_result['updated']
                result['sitemaps_saved'] = sitemap_save_result['created'] + sitemap_save_result['updated']
                result['saved_to_db'] = total_created
                result['updated_in_db'] = total_updated
                result['validation_failures'] = query_save_result.get('validation_failures', 0)

                # Update log with final counts after save completes
                _update_sync_log(sync_log_id, sync_result)

        return result

    def _save_search_queries(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save Search Console queries to database with validation.

        Returns:
            Dict with keys: processed, created, updated, failed, validation_failures
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0, 'validation_failures': 0}

        # Handle nested structure: data['query_performance']['queries']
        query_perf = data.get('query_performance', {})
        if isinstance(query_perf, dict):
            queries = query_perf.get('queries', [])
        else:
            queries = query_perf or data.get('queries', [])

        if not queries:
            return result

        db = SessionLocal()
        today = datetime.utcnow().date()

        try:
            for query_data in queries:
                query_text = query_data.get('query')

                # Validate the query data
                validation_result = validation_service.validate_search_query(query_data)
                if validation_result.all_issues:
                    result['validation_failures'] += validation_service.persist_validation_failures(
                        failures=validation_result.all_issues,
                        entity_type="search_query",
                        entity_id=query_text,
                        source="search_console",
                        sync_log_id=sync_log_id
                    )

                if validation_result.has_blocking_errors:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Use date if provided, otherwise use today (for aggregated data)
                    query_date = query_data.get('date')
                    if query_date:
                        if isinstance(query_date, str):
                            query_date = datetime.strptime(query_date, '%Y-%m-%d').date()
                    else:
                        query_date = today

                    # Check if exists
                    existing = db.query(SearchConsoleQuery).filter(
                        SearchConsoleQuery.query == query_text,
                        SearchConsoleQuery.date == query_date
                    ).first()

                    if existing:
                        existing.clicks = query_data.get('clicks', 0)
                        existing.impressions = query_data.get('impressions', 0)
                        existing.ctr = query_data.get('ctr', 0)
                        existing.position = query_data.get('position', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_query = SearchConsoleQuery(
                            date=query_date,
                            query=query_text,
                            page=query_data.get('page'),
                            device=query_data.get('device'),
                            country=query_data.get('country'),
                            clicks=query_data.get('clicks', 0),
                            impressions=query_data.get('impressions', 0),
                            ctr=query_data.get('ctr', 0),
                            position=query_data.get('position', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_query)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save query '{query_text}': {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Search Console queries (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_search_pages(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save Search Console pages to database with validation.

        Returns:
            Dict with keys: processed, created, updated, failed, validation_failures
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0, 'validation_failures': 0}

        # Handle nested structure: data['page_performance']['pages']
        page_perf = data.get('page_performance', {})
        if isinstance(page_perf, dict):
            pages = page_perf.get('pages', [])
        else:
            pages = page_perf or data.get('pages', [])

        if not pages:
            return result

        db = SessionLocal()
        today = datetime.utcnow().date()

        try:
            for page_data in pages:
                page_url = page_data.get('page')
                if not page_url:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Use date if provided, otherwise use today (for aggregated data)
                    page_date = page_data.get('date')
                    if page_date:
                        if isinstance(page_date, str):
                            page_date = datetime.strptime(page_date, '%Y-%m-%d').date()
                    else:
                        # Try to get from parent data (start_date from window)
                        start_date_str = data.get('start_date')
                        if start_date_str:
                            page_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                        else:
                            page_date = today

                    # Check if exists (unique key: page + date)
                    existing = db.query(SearchConsolePage).filter(
                        SearchConsolePage.page == page_url,
                        SearchConsolePage.date == page_date
                    ).first()

                    if existing:
                        existing.clicks = page_data.get('clicks', 0)
                        existing.impressions = page_data.get('impressions', 0)
                        existing.ctr = page_data.get('ctr', 0)
                        existing.position = page_data.get('position', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_page = SearchConsolePage(
                            date=page_date,
                            page=page_url,
                            device=page_data.get('device'),
                            country=page_data.get('country'),
                            clicks=page_data.get('clicks', 0),
                            impressions=page_data.get('impressions', 0),
                            ctr=page_data.get('ctr', 0),
                            position=page_data.get('position', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_page)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save page '{page_url}': {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Search Console pages (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_search_sitemaps(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save Search Console sitemaps to database.

        Returns:
            Dict with keys: processed, created, updated, failed
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0}

        # Handle nested structure: data['sitemaps'] is a list
        sitemaps = data.get('sitemaps', [])

        if not sitemaps:
            return result

        db = SessionLocal()

        try:
            for sitemap_data in sitemaps:
                sitemap_url = sitemap_data.get('url')
                if not sitemap_url:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Check if exists (unique key: sitemap_url)
                    existing = db.query(SearchConsoleSitemap).filter(
                        SearchConsoleSitemap.sitemap_url == sitemap_url
                    ).first()

                    # Parse datetime fields
                    def parse_datetime(value):
                        if not value:
                            return None
                        if isinstance(value, str):
                            try:
                                return datetime.fromisoformat(value.replace('Z', '+00:00'))
                            except ValueError:
                                return None
                        return value

                    last_submitted = parse_datetime(sitemap_data.get('last_submitted'))
                    last_downloaded = parse_datetime(sitemap_data.get('last_downloaded'))

                    if existing:
                        existing.submitted_urls = sitemap_data.get('submitted_urls', 0)
                        existing.indexed_urls = sitemap_data.get('indexed_urls', 0)
                        existing.is_pending = sitemap_data.get('is_pending', False)
                        existing.is_sitemaps_index = sitemap_data.get('is_sitemaps_index', False)
                        existing.errors = sitemap_data.get('errors', 0)
                        existing.warnings = sitemap_data.get('warnings', 0)
                        existing.last_submitted = last_submitted
                        existing.last_downloaded = last_downloaded
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_sitemap = SearchConsoleSitemap(
                            sitemap_url=sitemap_url,
                            submitted_urls=sitemap_data.get('submitted_urls', 0),
                            indexed_urls=sitemap_data.get('indexed_urls', 0),
                            is_pending=sitemap_data.get('is_pending', False),
                            is_sitemaps_index=sitemap_data.get('is_sitemaps_index', False),
                            errors=sitemap_data.get('errors', 0),
                            warnings=sitemap_data.get('warnings', 0),
                            last_submitted=last_submitted,
                            last_downloaded=last_downloaded,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_sitemap)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save sitemap '{sitemap_url}': {e}")
                    result['failed'] += 1
                    continue

            db.commit()
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Search Console sitemaps (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    async def backfill_search_console(
        self,
        months: int = 16,
        window_days: int = 14,
        delay_between_windows: float = 2.0
    ) -> Dict:
        """
        Backfill Search Console data in chunks to avoid rate limits.

        Args:
            months: Number of months to backfill (max 16)
            window_days: Days per fetch window (7-30)
            delay_between_windows: Seconds between windows

        Returns:
            Dict with backfill results
        """
        import asyncio
        backfill_start = time.time()

        log.info(f"Starting Search Console backfill: {months} months, {window_days}-day windows")

        # Create sync log for the entire backfill operation
        db = SessionLocal()
        try:
            sync_log = DataSyncLog(
                source="search_console",
                sync_type="backfill",
                status="running",
                started_at=datetime.utcnow()
            )
            db.add(sync_log)
            db.commit()
            sync_log_id = sync_log.id
        except Exception as e:
            log.error(f"Failed to create sync log: {e}")
            sync_log_id = None
        finally:
            db.close()

        # Calculate date range
        months = min(months, 16)
        end_date = datetime.now() - timedelta(days=3)  # GSC has 2-3 day delay
        start_date = datetime.now() - timedelta(days=months * 30)

        results = {
            "success": True,
            "sync_log_id": sync_log_id,
            "source": "Google Search Console",
            "sync_type": "backfill",
            "months_requested": months,
            "start_date": start_date.date().isoformat(),
            "end_date": end_date.date().isoformat(),
            "window_days": window_days,
            "windows_processed": 0,
            "windows_failed": 0,
            "total_queries_saved": 0,
            "total_pages_saved": 0,
            "window_results": [],
            "errors": []
        }

        try:
            # Connect to Search Console
            if not self.search_console.service:
                await self.search_console.connect()

            # Process windows
            current_start = start_date
            window_num = 0

            while current_start < end_date:
                window_num += 1
                window_end = min(current_start + timedelta(days=window_days), end_date)

                log.info(f"Processing window {window_num}: {current_start.date()} to {window_end.date()}")

                try:
                    # Fetch data for this window
                    window_result = await self.search_console._sync_window_with_retry(
                        current_start, window_end, max_retries=3
                    )

                    if window_result.get("success") and window_result.get("data"):
                        # Save the fetched data to database
                        window_data = window_result["data"]

                        # Save queries
                        query_save_result = self._save_search_queries(window_data, sync_log_id=sync_log_id)
                        queries_saved = query_save_result.get("created", 0) + query_save_result.get("updated", 0)

                        # Save pages
                        page_save_result = self._save_search_pages(window_data, sync_log_id=sync_log_id)
                        pages_saved = page_save_result.get("created", 0) + page_save_result.get("updated", 0)

                        results["windows_processed"] += 1
                        results["total_queries_saved"] += queries_saved
                        results["total_pages_saved"] += pages_saved

                        results["window_results"].append({
                            "window": window_num,
                            "start_date": current_start.date().isoformat(),
                            "end_date": window_end.date().isoformat(),
                            "success": True,
                            "queries_fetched": window_result.get("queries", 0),
                            "pages_fetched": window_result.get("pages", 0),
                            "queries_saved": queries_saved,
                            "pages_saved": pages_saved
                        })

                        log.info(f"Window {window_num} complete: saved {queries_saved} queries, {pages_saved} pages")
                    else:
                        results["windows_failed"] += 1
                        error_msg = window_result.get("error", "Unknown error")
                        results["errors"].append({
                            "window": window_num,
                            "dates": f"{current_start.date()} to {window_end.date()}",
                            "error": error_msg
                        })
                        results["window_results"].append({
                            "window": window_num,
                            "start_date": current_start.date().isoformat(),
                            "end_date": window_end.date().isoformat(),
                            "success": False,
                            "error": error_msg
                        })
                        log.warning(f"Window {window_num} failed: {error_msg}")

                except Exception as e:
                    results["windows_failed"] += 1
                    error_msg = str(e)
                    results["errors"].append({
                        "window": window_num,
                        "dates": f"{current_start.date()} to {window_end.date()}",
                        "error": error_msg
                    })
                    log.error(f"Window {window_num} exception: {e}")

                # Move to next window
                current_start = window_end + timedelta(days=1)

                # Rate limit delay between windows
                if current_start < end_date and delay_between_windows > 0:
                    log.debug(f"Waiting {delay_between_windows}s before next window...")
                    await asyncio.sleep(delay_between_windows)

            results["duration_seconds"] = round(time.time() - backfill_start, 2)
            results["success"] = results["windows_failed"] == 0

            total_records = results["total_queries_saved"] + results["total_pages_saved"]
            log.info(
                f"Search Console backfill complete: "
                f"{results['windows_processed']}/{window_num} windows, "
                f"{results['total_queries_saved']} queries + {results['total_pages_saved']} pages saved in {results['duration_seconds']}s"
            )

            # Update sync log with final results
            if sync_log_id:
                db = SessionLocal()
                try:
                    sync_log = db.query(DataSyncLog).filter(DataSyncLog.id == sync_log_id).first()
                    if sync_log:
                        sync_log.status = "success" if results["success"] else "partial" if results["windows_processed"] > 0 else "failed"
                        sync_log.records_processed = total_records
                        sync_log.records_created = total_records
                        sync_log.records_failed = 0
                        sync_log.duration_seconds = results["duration_seconds"]
                        sync_log.completed_at = datetime.utcnow()
                        sync_log.error_details = {
                            "windows_total": window_num,
                            "windows_successful": results["windows_processed"],
                            "windows_failed": results["windows_failed"],
                            "queries_saved": results["total_queries_saved"],
                            "pages_saved": results["total_pages_saved"],
                            "window_errors": results["errors"] if results["errors"] else None
                        }
                        db.commit()
                except Exception as e:
                    log.error(f"Failed to update sync log: {e}")
                finally:
                    db.close()

            return results

        except Exception as e:
            error_msg = f"Search Console backfill failed: {str(e)}"
            log.error(error_msg)

            # Update sync log with failure
            if sync_log_id:
                db = SessionLocal()
                try:
                    sync_log = db.query(DataSyncLog).filter(DataSyncLog.id == sync_log_id).first()
                    if sync_log:
                        sync_log.status = "failed"
                        sync_log.error_message = error_msg
                        sync_log.completed_at = datetime.utcnow()
                        sync_log.duration_seconds = round(time.time() - backfill_start, 2)
                        db.commit()
                except Exception:
                    pass
                finally:
                    db.close()

            return {
                "success": False,
                "source": "Google Search Console",
                "sync_type": "backfill",
                "error": error_msg,
                "sync_log_id": sync_log_id
            }

    async def daily_sync_search_console(self, days: int = 3) -> Dict:
        """
        Daily incremental Search Console sync.

        Args:
            days: Number of recent days to sync (1-7)

        Returns:
            Dict with sync results
        """
        sync_start = time.time()
        log.info(f"Starting Search Console daily sync: last {days} days")

        # Create sync log
        db = SessionLocal()
        try:
            sync_log = DataSyncLog(
                source="search_console",
                sync_type="daily",
                status="running",
                started_at=datetime.utcnow()
            )
            db.add(sync_log)
            db.commit()
            sync_log_id = sync_log.id
        except Exception as e:
            log.error(f"Failed to create sync log: {e}")
            sync_log_id = None
        finally:
            db.close()

        try:
            # Fetch data from connector
            result = await self.search_console.daily_sync(days=days)

            if not result.get("success"):
                raise Exception(result.get("error", "Unknown error"))

            # Save data to database
            data = result.get("data", {})

            # Save queries
            query_save_result = self._save_search_queries(data, sync_log_id=sync_log_id)
            queries_saved = query_save_result.get("created", 0) + query_save_result.get("updated", 0)

            # Save pages
            page_save_result = self._save_search_pages(data, sync_log_id=sync_log_id)
            pages_saved = page_save_result.get("created", 0) + page_save_result.get("updated", 0)

            # Save sitemaps
            sitemap_save_result = self._save_search_sitemaps(data, sync_log_id=sync_log_id)
            sitemaps_saved = sitemap_save_result.get("created", 0) + sitemap_save_result.get("updated", 0)

            total_records_saved = queries_saved + pages_saved + sitemaps_saved
            duration = round(time.time() - sync_start, 2)

            final_result = {
                "success": True,
                "sync_log_id": sync_log_id,
                "source": "Google Search Console",
                "sync_type": "daily",
                "days_synced": days,
                "start_date": result.get("start_date"),
                "end_date": result.get("end_date"),
                "queries_fetched": result.get("queries", 0),
                "pages_fetched": result.get("pages", 0),
                "sitemaps_fetched": result.get("sitemaps", 0),
                "queries_saved": queries_saved,
                "pages_saved": pages_saved,
                "sitemaps_saved": sitemaps_saved,
                "total_records_saved": total_records_saved,
                "duration_seconds": duration
            }

            # Update sync log
            if sync_log_id:
                db = SessionLocal()
                try:
                    sync_log = db.query(DataSyncLog).filter(DataSyncLog.id == sync_log_id).first()
                    if sync_log:
                        sync_log.status = "success"
                        sync_log.records_processed = result.get("queries", 0) + result.get("pages", 0) + result.get("sitemaps", 0)
                        sync_log.records_created = query_save_result.get("created", 0) + page_save_result.get("created", 0) + sitemap_save_result.get("created", 0)
                        sync_log.records_updated = query_save_result.get("updated", 0) + page_save_result.get("updated", 0) + sitemap_save_result.get("updated", 0)
                        sync_log.duration_seconds = duration
                        sync_log.completed_at = datetime.utcnow()
                        sync_log.error_details = {
                            "queries_saved": queries_saved,
                            "pages_saved": pages_saved,
                            "sitemaps_saved": sitemaps_saved
                        }
                        db.commit()
                except Exception as e:
                    log.error(f"Failed to update sync log: {e}")
                finally:
                    db.close()

            log.info(f"Search Console daily sync complete: {queries_saved} queries + {pages_saved} pages + {sitemaps_saved} sitemaps saved in {duration}s")

            # Update data_sync_status for freshness tracking
            try:
                status_result = SyncResult(
                    source="search_console",
                    sync_type="daily",
                    status="success",
                    records_processed=result.get("queries", 0) + result.get("pages", 0) + result.get("sitemaps", 0),
                    records_created=query_save_result.get("created", 0) + page_save_result.get("created", 0) + sitemap_save_result.get("created", 0),
                    records_updated=query_save_result.get("updated", 0) + page_save_result.get("updated", 0) + sitemap_save_result.get("updated", 0),
                    started_at=datetime.utcfromtimestamp(sync_start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=duration,
                )
                update_data_sync_status(status_result)
            except Exception:
                pass  # Never let status tracking break the sync

            return final_result

        except Exception as e:
            error_msg = f"Search Console daily sync failed: {str(e)}"
            log.error(error_msg)

            # Update sync log with failure
            if sync_log_id:
                db = SessionLocal()
                try:
                    sync_log = db.query(DataSyncLog).filter(DataSyncLog.id == sync_log_id).first()
                    if sync_log:
                        sync_log.status = "failed"
                        sync_log.error_message = error_msg
                        sync_log.completed_at = datetime.utcnow()
                        sync_log.duration_seconds = round(time.time() - sync_start, 2)
                        db.commit()
                except Exception:
                    pass
                finally:
                    db.close()

            # Update data_sync_status with failure
            try:
                status_result = SyncResult(
                    source="search_console",
                    sync_type="daily",
                    status="failed",
                    error_message=error_msg,
                    started_at=datetime.utcfromtimestamp(sync_start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=round(time.time() - sync_start, 2),
                )
                update_data_sync_status(status_result)
            except Exception:
                pass

            return {
                "success": False,
                "source": "Google Search Console",
                "sync_type": "daily",
                "error": error_msg,
                "sync_log_id": sync_log_id
            }

    def _parse_ga4_date(self, date_str: str):
        """Parse GA4 date string (YYYYMMDD or YYYY-MM-DD) to date object"""
        if not date_str:
            return None
        try:
            if len(date_str) == 8:  # YYYYMMDD
                return datetime.strptime(date_str, '%Y%m%d').date()
            else:
                return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            log.warning(f"Could not parse GA4 date: {date_str}")
            return None

    def _save_ga4_data(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save all GA4 data types to database with validation.

        Persists:
        - traffic_overview: Daily site-wide metrics to GA4TrafficSource (source='(all)')
        - traffic_sources: Per-day traffic by source/medium/campaign to GA4TrafficSource
        - pages: Page performance to GA4PagePerformance
        - landing_pages: Landing page performance to GA4LandingPage
        - products: Product performance to GA4ProductPerformance

        Returns:
            Dict with keys: processed, created, updated, failed, validation_failures
        """
        result = {
            'processed': 0, 'created': 0, 'updated': 0, 'failed': 0,
            'validation_failures': 0,
            'daily_summary_saved': 0,
            'traffic_overview_saved': 0,
            'traffic_sources_saved': 0,
            'pages_saved': 0,
            'landing_pages_saved': 0,
            'products_saved': 0,
            'events_saved': 0,
            'ecommerce_saved': 0,
            'device_breakdown_saved': 0,
            'geo_breakdown_saved': 0,
            'user_type_saved': 0,
        }

        db = SessionLocal()

        try:
            # 1. Save traffic overview (daily site-wide metrics)
            traffic_overview = data.get('traffic_overview', {})
            daily_metrics = traffic_overview.get('daily_metrics', [])

            for day_data in daily_metrics:
                date_str = day_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Upsert: check for existing record
                    existing = db.query(GA4TrafficSource).filter(
                        GA4TrafficSource.date == record_date,
                        GA4TrafficSource.session_source == '(all)',
                        GA4TrafficSource.session_medium == '(all)'
                    ).first()

                    if existing:
                        existing.sessions = day_data.get('sessions', 0)
                        existing.total_users = day_data.get('active_users', 0)
                        existing.new_users = day_data.get('new_users', 0)
                        existing.bounce_rate = day_data.get('bounce_rate', 0)
                        existing.avg_session_duration = day_data.get('avg_session_duration', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4TrafficSource(
                            date=record_date,
                            session_source='(all)',
                            session_medium='(all)',
                            sessions=day_data.get('sessions', 0),
                            total_users=day_data.get('active_users', 0),
                            new_users=day_data.get('new_users', 0),
                            bounce_rate=day_data.get('bounce_rate', 0),
                            avg_session_duration=day_data.get('avg_session_duration', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['traffic_overview_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 overview for {date_str}: {e}")
                    result['failed'] += 1

            db.commit()
            log.info(f"Saved {result['traffic_overview_saved']} GA4 daily overview records")

            # 2. Save traffic sources (with date dimension from each row)
            traffic_sources = data.get('traffic_sources', [])
            for source_data in traffic_sources:
                date_str = source_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                source = source_data.get('source', '(not set)')
                medium = source_data.get('medium', '(not set)')
                campaign = source_data.get('campaign', '(not set)')

                # Normalize "(not set)" values
                source = source if source != "(not set)" else None
                medium = medium if medium != "(not set)" else None
                campaign = campaign if campaign != "(not set)" else None

                try:
                    existing = db.query(GA4TrafficSource).filter(
                        GA4TrafficSource.date == record_date,
                        GA4TrafficSource.session_source == source,
                        GA4TrafficSource.session_medium == medium,
                        GA4TrafficSource.session_campaign_name == campaign
                    ).first()

                    if existing:
                        existing.sessions = source_data.get('sessions', 0)
                        existing.total_users = source_data.get('total_users', 0)
                        existing.new_users = source_data.get('new_users', 0)
                        existing.engaged_sessions = source_data.get('engaged_sessions', 0)
                        existing.bounce_rate = source_data.get('bounce_rate', 0)
                        existing.avg_session_duration = source_data.get('avg_session_duration', 0)
                        existing.conversions = source_data.get('conversions', 0)
                        existing.total_revenue = Decimal(str(source_data.get('revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4TrafficSource(
                            date=record_date,
                            session_source=source,
                            session_medium=medium,
                            session_campaign_name=campaign,
                            sessions=source_data.get('sessions', 0),
                            total_users=source_data.get('total_users', 0),
                            new_users=source_data.get('new_users', 0),
                            engaged_sessions=source_data.get('engaged_sessions', 0),
                            bounce_rate=source_data.get('bounce_rate', 0),
                            avg_session_duration=source_data.get('avg_session_duration', 0),
                            conversions=source_data.get('conversions', 0),
                            total_revenue=Decimal(str(source_data.get('revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['traffic_sources_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 traffic source: {e}")
                    result['failed'] += 1

                # Commit in batches
                if result['traffic_sources_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['traffic_sources_saved']} GA4 traffic source records")

            # 3. Save page performance
            pages = data.get('pages', [])
            for page_data in pages:
                date_str = page_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                page_path = page_data.get('path', '')
                page_title = page_data.get('title')
                page_title = page_title if page_title != "(not set)" else None

                try:
                    existing = db.query(GA4PagePerformance).filter(
                        GA4PagePerformance.date == record_date,
                        GA4PagePerformance.page_path == page_path
                    ).first()

                    if existing:
                        existing.page_title = page_title
                        existing.pageviews = page_data.get('pageviews', 0)
                        existing.unique_pageviews = page_data.get('sessions', 0)
                        existing.entrances = page_data.get('sessions', 0)
                        existing.bounce_rate = page_data.get('bounce_rate', 0)
                        existing.avg_time_on_page = page_data.get('avg_time_on_page', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4PagePerformance(
                            date=record_date,
                            page_path=page_path,
                            page_title=page_title,
                            pageviews=page_data.get('pageviews', 0),
                            unique_pageviews=page_data.get('sessions', 0),
                            entrances=page_data.get('sessions', 0),
                            bounce_rate=page_data.get('bounce_rate', 0),
                            avg_time_on_page=page_data.get('avg_time_on_page', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['pages_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 page: {e}")
                    result['failed'] += 1

                if result['pages_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['pages_saved']} GA4 page performance records")

            # 4. Save landing pages
            landing_pages = data.get('landing_pages', [])
            for lp_data in landing_pages:
                date_str = lp_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                landing_page = lp_data.get('landing_page', '')
                source = lp_data.get('source')
                medium = lp_data.get('medium')
                source = source if source != "(not set)" else None
                medium = medium if medium != "(not set)" else None

                try:
                    existing = db.query(GA4LandingPage).filter(
                        GA4LandingPage.date == record_date,
                        GA4LandingPage.landing_page == landing_page,
                        GA4LandingPage.session_source == source,
                        GA4LandingPage.session_medium == medium
                    ).first()

                    if existing:
                        existing.sessions = lp_data.get('sessions', 0)
                        existing.bounce_rate = lp_data.get('bounce_rate', 0)
                        existing.avg_session_duration = lp_data.get('avg_session_duration', 0)
                        existing.conversions = lp_data.get('conversions', 0)
                        existing.conversion_rate = lp_data.get('conversion_rate', 0)
                        existing.total_revenue = Decimal(str(lp_data.get('revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4LandingPage(
                            date=record_date,
                            landing_page=landing_page,
                            session_source=source,
                            session_medium=medium,
                            sessions=lp_data.get('sessions', 0),
                            bounce_rate=lp_data.get('bounce_rate', 0),
                            avg_session_duration=lp_data.get('avg_session_duration', 0),
                            conversions=lp_data.get('conversions', 0),
                            conversion_rate=lp_data.get('conversion_rate', 0),
                            total_revenue=Decimal(str(lp_data.get('revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['landing_pages_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 landing page: {e}")
                    result['failed'] += 1

                if result['landing_pages_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['landing_pages_saved']} GA4 landing page records")

            # 5. Save product performance
            products = data.get('products', [])
            for prod_data in products:
                date_str = prod_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                item_id = prod_data.get('item_id', '')
                item_name = prod_data.get('item_name')
                item_category = prod_data.get('item_category')
                item_name = item_name if item_name != "(not set)" else None
                item_category = item_category if item_category != "(not set)" else None

                try:
                    existing = db.query(GA4ProductPerformance).filter(
                        GA4ProductPerformance.date == record_date,
                        GA4ProductPerformance.item_id == item_id
                    ).first()

                    items_viewed = prod_data.get('items_viewed', 0)
                    items_purchased = prod_data.get('items_purchased', 0)

                    if existing:
                        existing.item_name = item_name
                        existing.item_category = item_category
                        existing.items_viewed = items_viewed
                        existing.items_added_to_cart = prod_data.get('items_added_to_cart', 0)
                        existing.items_purchased = items_purchased
                        existing.item_revenue = Decimal(str(prod_data.get('item_revenue', 0)))
                        existing.add_to_cart_rate = prod_data.get('add_to_cart_rate', 0)
                        existing.purchase_rate = items_purchased / items_viewed if items_viewed > 0 else 0
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4ProductPerformance(
                            date=record_date,
                            item_id=item_id,
                            item_name=item_name,
                            item_category=item_category,
                            items_viewed=items_viewed,
                            items_added_to_cart=prod_data.get('items_added_to_cart', 0),
                            items_purchased=items_purchased,
                            item_revenue=Decimal(str(prod_data.get('item_revenue', 0))),
                            add_to_cart_rate=prod_data.get('add_to_cart_rate', 0),
                            purchase_rate=items_purchased / items_viewed if items_viewed > 0 else 0,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['products_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 product: {e}")
                    result['failed'] += 1

                if result['products_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['products_saved']} GA4 product performance records")

            # 6. Save events/conversions (with date dimension for per-day tracking)
            conversions = data.get('conversions', [])
            for conv_data in conversions:
                date_str = conv_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                event_name = conv_data.get('event_name', '')

                try:
                    existing = db.query(GA4Event).filter(
                        GA4Event.date == record_date,
                        GA4Event.event_name == event_name
                    ).first()

                    if existing:
                        existing.event_count = conv_data.get('event_count', 0)
                        existing.total_users = conv_data.get('total_users', 0)
                        existing.total_revenue = Decimal(str(conv_data.get('revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4Event(
                            date=record_date,
                            event_name=event_name,
                            event_count=conv_data.get('event_count', 0),
                            total_users=conv_data.get('total_users', 0),
                            total_revenue=Decimal(str(conv_data.get('revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['events_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 event: {e}")
                    result['failed'] += 1

                if result['events_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['events_saved']} GA4 event records")

            # 7. Save daily ecommerce totals (uses ecommercePurchases for Shopify reconciliation)
            ecommerce = data.get('ecommerce', [])
            for ecom_data in ecommerce:
                date_str = ecom_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(GA4DailyEcommerce).filter(
                        GA4DailyEcommerce.date == record_date
                    ).first()

                    if existing:
                        existing.ecommerce_purchases = ecom_data.get('ecommerce_purchases', 0)
                        existing.total_revenue = Decimal(str(ecom_data.get('revenue', 0)))
                        existing.add_to_carts = ecom_data.get('add_to_carts', 0)
                        existing.checkouts = ecom_data.get('checkouts', 0)
                        existing.items_viewed = ecom_data.get('items_viewed', 0)
                        existing.cart_to_purchase_rate = ecom_data.get('cart_to_purchase_rate', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4DailyEcommerce(
                            date=record_date,
                            ecommerce_purchases=ecom_data.get('ecommerce_purchases', 0),
                            total_revenue=Decimal(str(ecom_data.get('revenue', 0))),
                            add_to_carts=ecom_data.get('add_to_carts', 0),
                            checkouts=ecom_data.get('checkouts', 0),
                            items_viewed=ecom_data.get('items_viewed', 0),
                            cart_to_purchase_rate=ecom_data.get('cart_to_purchase_rate', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['ecommerce_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 ecommerce: {e}")
                    result['failed'] += 1

            db.commit()
            log.info(f"Saved {result['ecommerce_saved']} GA4 daily ecommerce records")

            # 8. Save daily summary (comprehensive site-wide metrics)
            daily_summary = data.get('daily_summary', [])
            for day_data in daily_summary:
                date_str = day_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(GA4DailySummary).filter(
                        GA4DailySummary.date == record_date
                    ).first()

                    if existing:
                        existing.active_users = day_data.get('active_users', 0)
                        existing.new_users = day_data.get('new_users', 0)
                        existing.returning_users = day_data.get('returning_users', 0)
                        existing.sessions = day_data.get('sessions', 0)
                        existing.pageviews = day_data.get('pageviews', 0)
                        existing.engaged_sessions = day_data.get('engaged_sessions', 0)
                        existing.engagement_rate = day_data.get('engagement_rate', 0)
                        existing.bounce_rate = day_data.get('bounce_rate', 0)
                        existing.avg_session_duration = day_data.get('avg_session_duration', 0)
                        existing.avg_engagement_duration = day_data.get('avg_engagement_duration', 0)
                        existing.pages_per_session = day_data.get('pages_per_session', 0)
                        existing.events_per_session = day_data.get('events_per_session', 0)
                        existing.total_events = day_data.get('total_events', 0)
                        existing.total_conversions = day_data.get('total_conversions', 0)
                        existing.total_revenue = Decimal(str(day_data.get('total_revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4DailySummary(
                            date=record_date,
                            active_users=day_data.get('active_users', 0),
                            new_users=day_data.get('new_users', 0),
                            returning_users=day_data.get('returning_users', 0),
                            sessions=day_data.get('sessions', 0),
                            pageviews=day_data.get('pageviews', 0),
                            engaged_sessions=day_data.get('engaged_sessions', 0),
                            engagement_rate=day_data.get('engagement_rate', 0),
                            bounce_rate=day_data.get('bounce_rate', 0),
                            avg_session_duration=day_data.get('avg_session_duration', 0),
                            avg_engagement_duration=day_data.get('avg_engagement_duration', 0),
                            pages_per_session=day_data.get('pages_per_session', 0),
                            events_per_session=day_data.get('events_per_session', 0),
                            total_events=day_data.get('total_events', 0),
                            total_conversions=day_data.get('total_conversions', 0),
                            total_revenue=Decimal(str(day_data.get('total_revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['daily_summary_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 daily summary for {date_str}: {e}")
                    result['failed'] += 1

            db.commit()
            log.info(f"Saved {result['daily_summary_saved']} GA4 daily summary records")

            # 9. Save device breakdown
            device_data = data.get('device_breakdown', [])
            for dev_data in device_data:
                date_str = dev_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                device_category = dev_data.get('device_category', 'unknown')

                try:
                    existing = db.query(GA4DeviceBreakdown).filter(
                        GA4DeviceBreakdown.date == record_date,
                        GA4DeviceBreakdown.device_category == device_category
                    ).first()

                    if existing:
                        existing.sessions = dev_data.get('sessions', 0)
                        existing.active_users = dev_data.get('active_users', 0)
                        existing.new_users = dev_data.get('new_users', 0)
                        existing.engaged_sessions = dev_data.get('engaged_sessions', 0)
                        existing.bounce_rate = dev_data.get('bounce_rate', 0)
                        existing.avg_session_duration = dev_data.get('avg_session_duration', 0)
                        existing.conversions = dev_data.get('conversions', 0)
                        existing.total_revenue = Decimal(str(dev_data.get('total_revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4DeviceBreakdown(
                            date=record_date,
                            device_category=device_category,
                            sessions=dev_data.get('sessions', 0),
                            active_users=dev_data.get('active_users', 0),
                            new_users=dev_data.get('new_users', 0),
                            engaged_sessions=dev_data.get('engaged_sessions', 0),
                            bounce_rate=dev_data.get('bounce_rate', 0),
                            avg_session_duration=dev_data.get('avg_session_duration', 0),
                            conversions=dev_data.get('conversions', 0),
                            total_revenue=Decimal(str(dev_data.get('total_revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['device_breakdown_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 device breakdown: {e}")
                    result['failed'] += 1

            db.commit()
            log.info(f"Saved {result['device_breakdown_saved']} GA4 device breakdown records")

            # 10. Save geo breakdown
            geo_data = data.get('geo_breakdown', [])
            for g_data in geo_data:
                date_str = g_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                country = g_data.get('country', 'unknown')
                region = g_data.get('region')
                city = g_data.get('city')

                # Normalize "(not set)" values
                region = region if region and region != "(not set)" else None
                city = city if city and city != "(not set)" else None

                try:
                    existing = db.query(GA4GeoBreakdown).filter(
                        GA4GeoBreakdown.date == record_date,
                        GA4GeoBreakdown.country == country,
                        GA4GeoBreakdown.region == region,
                        GA4GeoBreakdown.city == city
                    ).first()

                    if existing:
                        existing.sessions = g_data.get('sessions', 0)
                        existing.active_users = g_data.get('active_users', 0)
                        existing.new_users = g_data.get('new_users', 0)
                        existing.engaged_sessions = g_data.get('engaged_sessions', 0)
                        existing.bounce_rate = g_data.get('bounce_rate', 0)
                        existing.conversions = g_data.get('conversions', 0)
                        existing.total_revenue = Decimal(str(g_data.get('total_revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4GeoBreakdown(
                            date=record_date,
                            country=country,
                            region=region,
                            city=city,
                            sessions=g_data.get('sessions', 0),
                            active_users=g_data.get('active_users', 0),
                            new_users=g_data.get('new_users', 0),
                            engaged_sessions=g_data.get('engaged_sessions', 0),
                            bounce_rate=g_data.get('bounce_rate', 0),
                            conversions=g_data.get('conversions', 0),
                            total_revenue=Decimal(str(g_data.get('total_revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['geo_breakdown_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 geo breakdown: {e}")
                    result['failed'] += 1

                if result['geo_breakdown_saved'] % 100 == 0:
                    db.commit()

            db.commit()
            log.info(f"Saved {result['geo_breakdown_saved']} GA4 geo breakdown records")

            # 11. Save user type breakdown (new vs returning)
            user_type_data = data.get('user_type_breakdown', [])
            for ut_data in user_type_data:
                date_str = ut_data.get('date')
                record_date = self._parse_ga4_date(date_str)
                if not record_date:
                    result['failed'] += 1
                    continue

                result['processed'] += 1
                user_type = ut_data.get('user_type', 'unknown')

                try:
                    existing = db.query(GA4UserType).filter(
                        GA4UserType.date == record_date,
                        GA4UserType.user_type == user_type
                    ).first()

                    if existing:
                        existing.users = ut_data.get('users', 0)
                        existing.sessions = ut_data.get('sessions', 0)
                        existing.engaged_sessions = ut_data.get('engaged_sessions', 0)
                        existing.pageviews = ut_data.get('pageviews', 0)
                        existing.avg_session_duration = ut_data.get('avg_session_duration', 0)
                        existing.conversions = ut_data.get('conversions', 0)
                        existing.total_revenue = Decimal(str(ut_data.get('total_revenue', 0)))
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_record = GA4UserType(
                            date=record_date,
                            user_type=user_type,
                            users=ut_data.get('users', 0),
                            sessions=ut_data.get('sessions', 0),
                            engaged_sessions=ut_data.get('engaged_sessions', 0),
                            pageviews=ut_data.get('pageviews', 0),
                            avg_session_duration=ut_data.get('avg_session_duration', 0),
                            conversions=ut_data.get('conversions', 0),
                            total_revenue=Decimal(str(ut_data.get('total_revenue', 0))),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_record)
                        result['created'] += 1

                    result['user_type_saved'] += 1

                except Exception as e:
                    log.warning(f"Failed to save GA4 user type: {e}")
                    result['failed'] += 1

            db.commit()
            log.info(f"Saved {result['user_type_saved']} GA4 user type records")

            log.info(
                f"GA4 sync complete: {result['created']} created, {result['updated']} updated, "
                f"{result['failed']} failed. "
                f"Summary: {result['daily_summary_saved']}, Sources: {result['traffic_sources_saved']}, "
                f"Pages: {result['pages_saved']}, Landing: {result['landing_pages_saved']}, "
                f"Products: {result['products_saved']}, Events: {result['events_saved']}, "
                f"Ecommerce: {result['ecommerce_saved']}, Device: {result['device_breakdown_saved']}, "
                f"Geo: {result['geo_breakdown_saved']}, UserType: {result['user_type_saved']}"
            )

            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving GA4 data (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_klaviyo_data(self, data: Dict, sync_log_id: int = None) -> Dict:
        """
        Save Klaviyo campaigns, flows, and segments to database with validation.

        Returns:
            Dict with keys: processed, created, updated, failed, validation_failures
        """
        result = {'processed': 0, 'created': 0, 'updated': 0, 'failed': 0, 'validation_failures': 0}
        db = SessionLocal()

        try:
            # Save campaigns
            campaigns = data.get('campaigns', [])
            for campaign_data in campaigns:
                campaign_id = campaign_data.get('id')

                # Validate campaign
                validation_result = validation_service.validate_klaviyo_campaign(campaign_data)
                if validation_result.all_issues:
                    result['validation_failures'] += validation_service.persist_validation_failures(
                        failures=validation_result.all_issues,
                        entity_type="campaign",
                        entity_id=campaign_id,
                        source="klaviyo",
                        sync_log_id=sync_log_id
                    )

                if validation_result.has_blocking_errors:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(KlaviyoCampaign).filter(
                        KlaviyoCampaign.campaign_id == campaign_id
                    ).first()

                    metrics = campaign_data.get('metrics', {})

                    if existing:
                        # Update existing
                        existing.campaign_name = campaign_data.get('name', existing.campaign_name)
                        existing.status = campaign_data.get('status')
                        existing.subject_line = campaign_data.get('subject')
                        existing.opens = metrics.get('opens', 0)
                        existing.unique_opens = metrics.get('unique_opens', 0)
                        existing.clicks = metrics.get('clicks', 0)
                        existing.unique_clicks = metrics.get('unique_clicks', 0)
                        existing.bounces = metrics.get('bounces', 0)
                        existing.spam_complaints = metrics.get('spam_complaints', 0)
                        existing.unsubscribes = metrics.get('unsubscribes', 0)
                        existing.open_rate = metrics.get('open_rate', 0)
                        existing.click_rate = metrics.get('click_rate', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        # Parse dates
                        send_time = None
                        if campaign_data.get('send_time'):
                            try:
                                send_time = datetime.fromisoformat(campaign_data['send_time'].replace('Z', '+00:00'))
                            except (ValueError, AttributeError) as e:
                                log.warning(f"Failed to parse Klaviyo send_time '{campaign_data.get('send_time')}': {e}")

                        created_at = None
                        if campaign_data.get('created_at'):
                            try:
                                created_at = datetime.fromisoformat(campaign_data['created_at'].replace('Z', '+00:00'))
                            except (ValueError, AttributeError) as e:
                                log.warning(f"Failed to parse Klaviyo created_at '{campaign_data.get('created_at')}': {e}")

                        new_campaign = KlaviyoCampaign(
                            campaign_id=campaign_id,
                            campaign_name=campaign_data.get('name', 'Untitled'),
                            status=campaign_data.get('status'),
                            subject_line=campaign_data.get('subject'),
                            send_time=send_time,
                            created_at_klaviyo=created_at,
                            recipients=metrics.get('sent', 0),
                            opens=metrics.get('opens', 0),
                            unique_opens=metrics.get('unique_opens', 0),
                            clicks=metrics.get('clicks', 0),
                            unique_clicks=metrics.get('unique_clicks', 0),
                            bounces=metrics.get('bounces', 0),
                            spam_complaints=metrics.get('spam_complaints', 0),
                            unsubscribes=metrics.get('unsubscribes', 0),
                            open_rate=metrics.get('open_rate', 0),
                            click_rate=metrics.get('click_rate', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_campaign)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save campaign {campaign_id}: {e}")
                    result['failed'] += 1

            # Save flows
            flows = data.get('flows', [])
            for flow_data in flows:
                flow_id = flow_data.get('id')

                # Validate flow
                validation_result = validation_service.validate_klaviyo_flow(flow_data)
                if validation_result.all_issues:
                    result['validation_failures'] += validation_service.persist_validation_failures(
                        failures=validation_result.all_issues,
                        entity_type="flow",
                        entity_id=flow_id,
                        source="klaviyo",
                        sync_log_id=sync_log_id
                    )

                if validation_result.has_blocking_errors:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(KlaviyoFlow).filter(
                        KlaviyoFlow.flow_id == flow_id
                    ).first()

                    if existing:
                        existing.flow_name = flow_data.get('name', existing.flow_name)
                        existing.status = flow_data.get('status')
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        created_at = None
                        if flow_data.get('created_at'):
                            try:
                                created_at = datetime.fromisoformat(flow_data['created_at'].replace('Z', '+00:00'))
                            except (ValueError, AttributeError):
                                pass

                        updated_at = None
                        if flow_data.get('updated_at'):
                            try:
                                updated_at = datetime.fromisoformat(flow_data['updated_at'].replace('Z', '+00:00'))
                            except (ValueError, AttributeError):
                                pass

                        new_flow = KlaviyoFlow(
                            flow_id=flow_id,
                            flow_name=flow_data.get('name', 'Untitled'),
                            status=flow_data.get('status'),
                            created_at_klaviyo=created_at,
                            updated_at_klaviyo=updated_at,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_flow)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save flow {flow_id}: {e}")
                    result['failed'] += 1

            # Save flow messages
            flow_messages = data.get('flow_messages', [])
            for msg_data in flow_messages:
                message_id = msg_data.get('message_id')
                if not message_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(KlaviyoFlowMessage).filter(
                        KlaviyoFlowMessage.message_id == message_id
                    ).first()

                    metrics = msg_data.get('metrics', {})

                    # Calculate rates
                    recipients = metrics.get('recipients', 0) or 0
                    opens = metrics.get('opens', 0) or 0
                    clicks = metrics.get('clicks', 0) or 0
                    conversions = metrics.get('conversions', 0) or 0

                    open_rate = (opens / recipients * 100) if recipients > 0 else None
                    click_rate = (clicks / recipients * 100) if recipients > 0 else None
                    conversion_rate = (conversions / recipients * 100) if recipients > 0 else None

                    if existing:
                        existing.message_name = msg_data.get('message_name', existing.message_name)
                        existing.subject_line = msg_data.get('subject_line')
                        existing.recipients = recipients
                        existing.opens = opens
                        existing.clicks = clicks
                        existing.conversions = conversions
                        existing.revenue = Decimal(str(metrics.get('revenue', 0) or 0))
                        existing.open_rate = open_rate
                        existing.click_rate = click_rate
                        existing.conversion_rate = conversion_rate
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_message = KlaviyoFlowMessage(
                            message_id=message_id,
                            flow_id=msg_data.get('flow_id'),
                            message_name=msg_data.get('message_name'),
                            subject_line=msg_data.get('subject_line'),
                            recipients=recipients,
                            opens=opens,
                            clicks=clicks,
                            conversions=conversions,
                            revenue=Decimal(str(metrics.get('revenue', 0) or 0)),
                            open_rate=open_rate,
                            click_rate=click_rate,
                            conversion_rate=conversion_rate,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_message)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save flow message {message_id}: {e}")
                    result['failed'] += 1

            # Save segments (both lists and segments)
            segments = data.get('segments', []) + data.get('lists', [])
            for segment_data in segments:
                segment_id = segment_data.get('id')

                # Validate segment
                validation_result = validation_service.validate_klaviyo_segment(segment_data)
                if validation_result.all_issues:
                    result['validation_failures'] += validation_service.persist_validation_failures(
                        failures=validation_result.all_issues,
                        entity_type="segment",
                        entity_id=segment_id,
                        source="klaviyo",
                        sync_log_id=sync_log_id
                    )

                if validation_result.has_blocking_errors:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(KlaviyoSegment).filter(
                        KlaviyoSegment.segment_id == segment_id
                    ).first()

                    if existing:
                        existing.segment_name = segment_data.get('name', existing.segment_name)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        created_at = None
                        if segment_data.get('created_at'):
                            try:
                                created_at = datetime.fromisoformat(segment_data['created_at'].replace('Z', '+00:00'))
                            except (ValueError, AttributeError):
                                pass

                        new_segment = KlaviyoSegment(
                            segment_id=segment_id,
                            segment_name=segment_data.get('name', 'Untitled'),
                            segment_type='segment' if segment_data in data.get('segments', []) else 'list',
                            created_at_klaviyo=created_at,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_segment)
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save segment {segment_id}: {e}")
                    result['failed'] += 1

            db.commit()
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Klaviyo data (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_google_ads_data(self, data: Dict, reference_date, sync_log_id: int = None) -> Dict:
        """
        Save Google Ads campaigns, ad groups, and search terms to database.

        Args:
            data: Data dict from connector with campaigns, ad_groups, search_terms
            reference_date: Date to use for records (connector returns aggregated data)
            sync_log_id: Optional sync log ID for tracking

        Returns:
            Dict with keys: processed, created, updated, failed, and per-entity counts
        """
        result = {
            'processed': 0, 'created': 0, 'updated': 0, 'failed': 0,
            'campaigns_created': 0, 'campaigns_updated': 0,
            'ad_groups_created': 0, 'ad_groups_updated': 0,
            'search_terms_created': 0, 'search_terms_updated': 0
        }
        db = SessionLocal()

        try:
            # Save campaigns
            campaigns = data.get('campaigns', [])
            for campaign_data in campaigns:
                campaign_id = str(campaign_data.get('id'))
                if not campaign_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Check if exists for this date
                    existing = db.query(GoogleAdsCampaign).filter(
                        GoogleAdsCampaign.campaign_id == campaign_id,
                        GoogleAdsCampaign.date == reference_date
                    ).first()

                    # Convert cost from dollars to micros for storage
                    cost_dollars = campaign_data.get('cost', 0)
                    cost_micros = int(cost_dollars * 1_000_000) if cost_dollars else 0

                    if existing:
                        # Update existing
                        existing.campaign_name = campaign_data.get('name', existing.campaign_name)
                        existing.campaign_type = campaign_data.get('channel_type')
                        existing.campaign_status = campaign_data.get('status')
                        existing.impressions = campaign_data.get('impressions', 0)
                        existing.clicks = campaign_data.get('clicks', 0)
                        existing.cost_micros = cost_micros
                        existing.conversions = campaign_data.get('conversions', 0)
                        existing.conversions_value = campaign_data.get('conversion_value', 0)
                        existing.ctr = campaign_data.get('ctr', 0)
                        existing.avg_cpc = campaign_data.get('avg_cpc', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                        result['campaigns_updated'] += 1
                    else:
                        # Create new
                        new_campaign = GoogleAdsCampaign(
                            campaign_id=campaign_id,
                            campaign_name=campaign_data.get('name', 'Unknown'),
                            campaign_type=campaign_data.get('channel_type'),
                            campaign_status=campaign_data.get('status'),
                            date=reference_date,
                            impressions=campaign_data.get('impressions', 0),
                            clicks=campaign_data.get('clicks', 0),
                            cost_micros=cost_micros,
                            conversions=campaign_data.get('conversions', 0),
                            conversions_value=campaign_data.get('conversion_value', 0),
                            ctr=campaign_data.get('ctr', 0),
                            avg_cpc=campaign_data.get('avg_cpc', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_campaign)
                        result['created'] += 1
                        result['campaigns_created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save campaign {campaign_id}: {e}")
                    result['failed'] += 1

            # Save ad groups
            ad_groups = data.get('ad_groups', [])
            for ag_data in ad_groups:
                ad_group_id = str(ag_data.get('id'))
                if not ad_group_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    existing = db.query(GoogleAdsAdGroup).filter(
                        GoogleAdsAdGroup.ad_group_id == ad_group_id,
                        GoogleAdsAdGroup.date == reference_date
                    ).first()

                    cost_dollars = ag_data.get('cost', 0)
                    cost_micros = int(cost_dollars * 1_000_000) if cost_dollars else 0

                    # Find campaign_id from campaign name if available
                    campaign_name = ag_data.get('campaign', '')
                    campaign_id = ''
                    for c in campaigns:
                        if c.get('name') == campaign_name:
                            campaign_id = str(c.get('id', ''))
                            break

                    if existing:
                        existing.ad_group_name = ag_data.get('name', existing.ad_group_name)
                        existing.ad_group_status = ag_data.get('status')
                        existing.campaign_id = campaign_id or existing.campaign_id
                        existing.impressions = ag_data.get('impressions', 0)
                        existing.clicks = ag_data.get('clicks', 0)
                        existing.cost_micros = cost_micros
                        existing.conversions = ag_data.get('conversions', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                        result['ad_groups_updated'] += 1
                    else:
                        new_ad_group = GoogleAdsAdGroup(
                            ad_group_id=ad_group_id,
                            ad_group_name=ag_data.get('name', 'Unknown'),
                            ad_group_status=ag_data.get('status'),
                            campaign_id=campaign_id,
                            date=reference_date,
                            impressions=ag_data.get('impressions', 0),
                            clicks=ag_data.get('clicks', 0),
                            cost_micros=cost_micros,
                            conversions=ag_data.get('conversions', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_ad_group)
                        result['created'] += 1
                        result['ad_groups_created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save ad group {ad_group_id}: {e}")
                    result['failed'] += 1

            # Save search terms
            search_terms = data.get('search_terms', [])
            for st_data in search_terms:
                search_term = st_data.get('search_term')
                if not search_term:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Find campaign_id and ad_group_id from names
                    campaign_name = st_data.get('campaign', '')
                    ad_group_name = st_data.get('ad_group', '')
                    campaign_id = ''
                    ad_group_id = ''

                    for c in campaigns:
                        if c.get('name') == campaign_name:
                            campaign_id = str(c.get('id', ''))
                            break

                    for ag in ad_groups:
                        if ag.get('name') == ad_group_name:
                            ad_group_id = str(ag.get('id', ''))
                            break

                    # Check if exists for this date, campaign, and ad group
                    existing = db.query(GoogleAdsSearchTerm).filter(
                        GoogleAdsSearchTerm.search_term == search_term,
                        GoogleAdsSearchTerm.campaign_id == campaign_id,
                        GoogleAdsSearchTerm.ad_group_id == ad_group_id,
                        GoogleAdsSearchTerm.date == reference_date
                    ).first()

                    cost_dollars = st_data.get('cost', 0)
                    cost_micros = int(cost_dollars * 1_000_000) if cost_dollars else 0

                    if existing:
                        existing.impressions = st_data.get('impressions', 0)
                        existing.clicks = st_data.get('clicks', 0)
                        existing.cost_micros = cost_micros
                        existing.conversions = st_data.get('conversions', 0)
                        existing.synced_at = datetime.utcnow()
                        result['updated'] += 1
                        result['search_terms_updated'] += 1
                    else:
                        new_search_term = GoogleAdsSearchTerm(
                            search_term=search_term,
                            campaign_id=campaign_id,
                            ad_group_id=ad_group_id,
                            date=reference_date,
                            impressions=st_data.get('impressions', 0),
                            clicks=st_data.get('clicks', 0),
                            cost_micros=cost_micros,
                            conversions=st_data.get('conversions', 0),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_search_term)
                        result['created'] += 1
                        result['search_terms_created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save search term '{search_term}': {e}")
                    result['failed'] += 1

            db.commit()
            log.info(
                f"Google Ads saved: {result['campaigns_created']} campaigns, "
                f"{result['ad_groups_created']} ad groups, {result['search_terms_created']} search terms"
            )
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Google Ads data (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    def _save_merchant_center_data(self, data: Dict, snapshot_date) -> Dict:
        """
        Save Merchant Center product statuses, disapprovals, and account status to database.

        Args:
            data: Data dict from connector with product_statuses, account_status
            snapshot_date: Date for this snapshot

        Returns:
            Dict with keys: processed, created, updated, failed, and per-entity counts
        """
        result = {
            'processed': 0, 'created': 0, 'updated': 0, 'failed': 0,
            'statuses_created': 0, 'statuses_updated': 0,
            'disapprovals_created': 0, 'disapprovals_updated': 0,
            'account_status_saved': False
        }
        db = SessionLocal()

        try:
            # Save account status summary
            account_status = data.get('account_status', {})
            product_statuses = data.get('product_statuses', {})

            if account_status or product_statuses:
                result['processed'] += 1

                try:
                    # Check if we already have a record for today
                    existing_account = db.query(MerchantCenterAccountStatus).filter(
                        MerchantCenterAccountStatus.snapshot_date == snapshot_date
                    ).first()

                    total_products = (
                        product_statuses.get('approved', 0) +
                        product_statuses.get('disapproved', 0) +
                        product_statuses.get('pending', 0)
                    )
                    approved = product_statuses.get('approved', 0)
                    approval_rate = (approved / total_products * 100) if total_products > 0 else None

                    if existing_account:
                        existing_account.total_products = total_products
                        existing_account.approved_count = approved
                        existing_account.disapproved_count = product_statuses.get('disapproved', 0)
                        existing_account.pending_count = product_statuses.get('pending', 0)
                        existing_account.expiring_count = product_statuses.get('expiring', 0)
                        existing_account.approval_rate = approval_rate
                        existing_account.account_issue_count = len(account_status.get('account_issues', []))
                        existing_account.account_issues = account_status.get('account_issues')
                        existing_account.website_claimed = account_status.get('website_claimed', False)
                        existing_account.synced_at = datetime.utcnow()
                        result['updated'] += 1
                    else:
                        new_account_status = MerchantCenterAccountStatus(
                            snapshot_date=snapshot_date,
                            total_products=total_products,
                            approved_count=approved,
                            disapproved_count=product_statuses.get('disapproved', 0),
                            pending_count=product_statuses.get('pending', 0),
                            expiring_count=product_statuses.get('expiring', 0),
                            approval_rate=approval_rate,
                            account_issue_count=len(account_status.get('account_issues', [])),
                            account_issues=account_status.get('account_issues'),
                            website_claimed=account_status.get('website_claimed', False),
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_account_status)
                        result['created'] += 1

                    result['account_status_saved'] = True

                except Exception as e:
                    log.warning(f"Failed to save account status: {e}")
                    result['failed'] += 1

            # Save ALL product statuses (approved and disapproved)
            # This enables tracking approved→disapproved transitions over time
            all_products = product_statuses.get('all_products', [])
            products_with_issues = product_statuses.get('products_with_issues', [])

            # Build a lookup for products with issues (for detailed issue info)
            products_issues_lookup = {
                p.get('product_id'): p.get('issues', [])
                for p in products_with_issues
            }

            for product in all_products:
                product_id = product.get('product_id')
                if not product_id:
                    result['failed'] += 1
                    continue

                result['processed'] += 1

                try:
                    # Use approval_status from the product data (already determined by connector)
                    approval_status = product.get('approval_status', 'pending')
                    has_issues = product.get('has_issues', False)
                    issue_count = product.get('issue_count', 0)

                    # Count critical issues from the detailed issues list if available
                    issues = products_issues_lookup.get(product_id, [])
                    critical_count = sum(
                        1 for issue in issues
                        if issue.get('severity') == 'disapproved'
                    )

                    # Save product status
                    existing_status = db.query(MerchantCenterProductStatus).filter(
                        MerchantCenterProductStatus.product_id == product_id,
                        MerchantCenterProductStatus.snapshot_date == snapshot_date
                    ).first()

                    if existing_status:
                        existing_status.approval_status = approval_status
                        existing_status.has_issues = has_issues
                        existing_status.issue_count = issue_count
                        existing_status.critical_issue_count = critical_count
                        existing_status.synced_at = datetime.utcnow()
                        result['statuses_updated'] += 1
                        result['updated'] += 1
                    else:
                        new_status = MerchantCenterProductStatus(
                            product_id=product_id,
                            title=product.get('title'),
                            snapshot_date=snapshot_date,
                            approval_status=approval_status,
                            has_issues=has_issues,
                            issue_count=issue_count,
                            critical_issue_count=critical_count,
                            synced_at=datetime.utcnow()
                        )
                        db.add(new_status)
                        result['statuses_created'] += 1
                        result['created'] += 1

                except Exception as e:
                    log.warning(f"Failed to save product status {product_id}: {e}")
                    result['failed'] += 1

            # Save detailed disapproval records only for products with issues
            for product in products_with_issues:
                product_id = product.get('product_id')
                if not product_id:
                    continue

                issues = product.get('issues', [])

                for issue in issues:
                    issue_code = issue.get('code')
                    if not issue_code:
                        continue

                    result['processed'] += 1

                    try:
                        existing_disapproval = db.query(MerchantCenterDisapproval).filter(
                            MerchantCenterDisapproval.product_id == product_id,
                            MerchantCenterDisapproval.issue_code == issue_code,
                            MerchantCenterDisapproval.snapshot_date == snapshot_date
                        ).first()

                        # Find first seen date for this issue
                        first_seen = db.query(MerchantCenterDisapproval).filter(
                            MerchantCenterDisapproval.product_id == product_id,
                            MerchantCenterDisapproval.issue_code == issue_code
                        ).order_by(MerchantCenterDisapproval.snapshot_date.asc()).first()

                        first_seen_date = first_seen.snapshot_date if first_seen else snapshot_date

                        if existing_disapproval:
                            existing_disapproval.issue_severity = issue.get('severity')
                            existing_disapproval.issue_description = issue.get('description')
                            existing_disapproval.issue_detail = issue.get('detail')
                            existing_disapproval.synced_at = datetime.utcnow()
                            result['disapprovals_updated'] += 1
                            result['updated'] += 1
                        else:
                            new_disapproval = MerchantCenterDisapproval(
                                product_id=product_id,
                                title=product.get('title'),
                                snapshot_date=snapshot_date,
                                issue_code=issue_code,
                                issue_severity=issue.get('severity'),
                                issue_description=issue.get('description'),
                                issue_detail=issue.get('detail'),
                                issue_attribute=issue.get('attribute'),
                                issue_destination=issue.get('destination'),
                                documentation_url=issue.get('documentation'),
                                first_seen_date=first_seen_date,
                                is_resolved=False,
                                synced_at=datetime.utcnow()
                            )
                            db.add(new_disapproval)
                            result['disapprovals_created'] += 1
                            result['created'] += 1

                    except Exception as e:
                        log.warning(f"Failed to save disapproval {product_id}/{issue_code}: {e}")
                        result['failed'] += 1

            db.commit()
            log.info(
                f"Merchant Center saved: {result['statuses_created']} product statuses, "
                f"{result['disapprovals_created']} disapprovals"
            )
            return result

        except Exception as e:
            db.rollback()
            log.error(f"Error saving Merchant Center data (batch failed): {e}")
            result['failed'] = result['processed']
            result['created'] = 0
            result['updated'] = 0
            return result
        finally:
            db.close()

    async def sync_shippit(self, days: int = 7) -> Dict:
        """
        Sync Shippit shipping cost data.

        1. Find fulfilled Shopify orders without a ShippitOrder record
        2. Fetch tracking numbers from Shopify fulfillments API
        3. Look up each tracking number in Shippit to get shipping cost
        """
        if not self.shippit:
            return {"success": False, "error": "Shippit not configured (no API key)"}

        with track_sync("shippit", "incremental") as sync_result:
            start_date, end_date = self._get_sydney_date_range(days)

            # Find fulfilled Shopify orders without Shippit cost data
            db = SessionLocal()
            try:
                from sqlalchemy import or_

                fulfilled_orders = (
                    db.query(ShopifyOrder.order_number, ShopifyOrder.shopify_order_id)
                    .outerjoin(
                        ShippitOrder,
                        ShippitOrder.shopify_order_id == ShopifyOrder.shopify_order_id,
                    )
                    .filter(
                        ShopifyOrder.created_at >= start_date,
                        ShopifyOrder.fulfillment_status.in_(["fulfilled", "partial"]),
                        or_(
                            ShippitOrder.id.is_(None),
                            ShippitOrder.state.notin_(["delivered", "completed"]),
                        ),
                    )
                    .all()
                )
                order_ids = [
                    o.shopify_order_id for o in fulfilled_orders if o.shopify_order_id
                ]
            finally:
                db.close()

            if not order_ids:
                sync_result.records_processed = 0
                sync_log_id = _persist_sync_log(sync_result)
                return {
                    "success": True,
                    "orders_checked": 0,
                    "orders_saved": 0,
                    "duration": sync_result.duration_seconds,
                    "sync_log_id": sync_log_id,
                }

            # Step 2: Get tracking numbers from Shopify fulfillments
            log.info(f"Fetching tracking numbers for {len(order_ids)} fulfilled orders")
            tracking_map = await self.shopify.fetch_fulfillment_tracking(order_ids)

            all_tracking = []
            for tn_list in tracking_map.values():
                all_tracking.extend(tn_list)

            if not all_tracking:
                sync_result.records_processed = 0
                sync_log_id = _persist_sync_log(sync_result)
                return {
                    "success": True,
                    "orders_checked": len(order_ids),
                    "tracking_found": 0,
                    "orders_saved": 0,
                    "duration": sync_result.duration_seconds,
                    "sync_log_id": sync_log_id,
                }

            log.info(
                f"Found {len(all_tracking)} tracking numbers, "
                f"looking up in Shippit"
            )

            # Step 3: Look up tracking numbers in Shippit
            result = await self.shippit.sync(
                start_date, end_date, tracking_numbers=all_tracking
            )

            retry_stats = result.get("retry_stats", {})
            sync_result.retry_attempts = retry_stats.get("retries", 0) + 1
            sync_result.retry_delay_seconds = retry_stats.get("total_delay_seconds", 0)
            sync_result.retry_errors = retry_stats.get("errors", [])

            if not result.get("success"):
                sync_result.status = "failed"
                sync_result.error_message = result.get("error", "Unknown error")
                _persist_sync_log(sync_result)
                return result

            sync_log_id = _persist_sync_log(sync_result)
            result["sync_log_id"] = sync_log_id

            data = result.get("data", {})
            save_result = self._save_shippit_orders(data, sync_log_id=sync_log_id)

            sync_result.records_created = save_result["created"]
            sync_result.records_updated = save_result["updated"]
            sync_result.records_failed = save_result["failed"]
            sync_result.records_processed = save_result["processed"]

            _update_sync_log(sync_log_id, sync_result)

            result["orders_checked"] = len(order_ids)
            result["tracking_found"] = len(all_tracking)
            result["orders_saved"] = save_result["created"]
            result["orders_updated"] = save_result["updated"]
            result["duration"] = sync_result.duration_seconds

        return result

    def _save_shippit_orders(self, data: Dict, sync_log_id: int = None) -> Dict:
        """Save Shippit orders to database, resolving shopify_order_id."""
        result = {"processed": 0, "created": 0, "updated": 0, "failed": 0}

        orders = data.get("orders", [])
        if not orders:
            return result

        db = SessionLocal()
        try:
            for order_data in orders:
                tracking = order_data.get("tracking_number")
                if not tracking:
                    result["failed"] += 1
                    continue

                result["processed"] += 1

                try:
                    retailer_ref = order_data.get("retailer_order_number")
                    # Resolve shopify_order_id: prefer direct ID from retailer_reference
                    shopify_order_id = None
                    ref_str = order_data.get("shopify_order_id_from_ref", "")
                    if ref_str:
                        try:
                            shopify_order_id = int(ref_str)
                        except (ValueError, TypeError):
                            pass
                    # Fallback: resolve from retailer_invoice → order_number
                    if not shopify_order_id and retailer_ref:
                        try:
                            ref_int = int(retailer_ref.replace("INT", ""))
                            shopify_order = (
                                db.query(ShopifyOrder.shopify_order_id)
                                .filter(ShopifyOrder.order_number == ref_int)
                                .first()
                            )
                            if shopify_order:
                                shopify_order_id = shopify_order.shopify_order_id
                        except (ValueError, TypeError):
                            pass

                    # Upsert by tracking_number
                    existing = (
                        db.query(ShippitOrder)
                        .filter(ShippitOrder.tracking_number == tracking)
                        .first()
                    )

                    cost = order_data.get("shipping_cost")
                    shipping_cost = Decimal(str(cost)) if cost is not None else None

                    if existing:
                        existing.shipping_cost = shipping_cost
                        existing.state = order_data.get("state")
                        existing.courier_name = order_data.get("courier_name")
                        existing.courier_type = order_data.get("courier_type")
                        existing.shopify_order_id = (
                            shopify_order_id or existing.shopify_order_id
                        )
                        existing.raw_response = order_data.get("raw_response")
                        existing.synced_at = datetime.utcnow()
                        result["updated"] += 1
                    else:
                        new_order = ShippitOrder(
                            tracking_number=tracking,
                            retailer_order_number=retailer_ref,
                            shopify_order_id=shopify_order_id,
                            courier_name=order_data.get("courier_name"),
                            courier_type=order_data.get("courier_type"),
                            service_level=order_data.get("service_level"),
                            shipping_cost=shipping_cost,
                            state=order_data.get("state"),
                            parcel_count=order_data.get("parcel_count", 1),
                            created_at=self._parse_datetime(
                                order_data.get("created_at")
                            ),
                            delivered_at=self._parse_datetime(
                                order_data.get("delivered_at")
                            ),
                            raw_response=order_data.get("raw_response"),
                            synced_at=datetime.utcnow(),
                        )
                        db.add(new_order)
                        result["created"] += 1

                    if result["processed"] % 50 == 0:
                        db.commit()

                except Exception as e:
                    log.warning(f"Error saving Shippit order {tracking}: {e}")
                    db.rollback()
                    result["failed"] += 1

            db.commit()
        except Exception as e:
            db.rollback()
            log.error(f"Error saving Shippit orders: {e}")
        finally:
            db.close()

        log.info(
            f"Shippit save: {result['created']} created, "
            f"{result['updated']} updated, {result['failed']} failed"
        )
        return result

    def get_sync_status(self) -> Dict:
        """Get sync status for all connectors"""
        status = {
            'shopify': self.shopify.get_status(),
            'klaviyo': self.klaviyo.get_status(),
            'ga4': self.ga4.get_status(),
            'google_ads': self.google_ads.get_status(),
            'merchant_center': self.merchant_center.get_status(),
            'github': self.github.get_status(),
            'search_console': self.search_console.get_status(),
        }
        if self.shippit:
            status['shippit'] = self.shippit.get_status()
        return status
