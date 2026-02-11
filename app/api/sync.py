"""
Data synchronization endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from datetime import datetime, timedelta
from app.services.data_sync_service import DataSyncService
from app.services.caprice_import_service import CapriceImportService
from app.services.google_ads_import_service import GoogleAdsImportService
from app.services.google_ads_sheet_import import GoogleAdsSheetImportService, GOOGLE_AVAILABLE
from app.models.base import SessionLocal
from app.models.analytics import DataSyncLog
from app.models.caprice_import import CapriceImportLog
from app.models.google_ads_import import GoogleAdsImportLog
from app.utils.logger import log
from app.utils.cache import clear_cache

router = APIRouter(prefix="/sync", tags=["sync"])

data_sync = DataSyncService()


@router.post("/all")
async def sync_all_sources(days: int = Query(30, description="Number of days to sync")):
    """
    Sync data from all sources
    """
    try:
        result = await data_sync.sync_all(days=days)
        clear_cache()
        return result
    except Exception as e:
        log.error(f"Sync all error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/costs/test")
def test_cost_sheet_access():
    """
    Test Google Sheets connectivity — lists tab names without importing data.
    Use this to verify credentials and sheet ID are correct.
    """
    from app.connectors.google_sheets import GoogleSheetsConnector
    from app.config import get_settings
    settings = get_settings()
    db = SessionLocal()
    try:
        connector = GoogleSheetsConnector(
            db=db,
            credentials_path=settings.google_sheets_credentials_path,
            sheet_id=settings.cost_sheet_id,
            sheet_range=settings.cost_sheet_range,
        )
        # Build the service (authenticate)
        import asyncio
        authenticated = asyncio.run(connector.authenticate())
        if not authenticated:
            return {"success": False, "error": "Authentication failed — check credentials file and sheet ID"}
        tabs = connector._get_all_sheet_titles()
        return {
            "success": True,
            "sheet_id": settings.cost_sheet_id,
            "credentials_path": settings.google_sheets_credentials_path,
            "tabs_found": len(tabs),
            "tab_names": tabs,
        }
    except (FileNotFoundError, ValueError, TimeoutError) as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error(f"Cost sheet test error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/costs")
async def sync_cost_sheet():
    """
    Sync NETT Master Sheet (Google Sheets) into product_costs table.

    Example: POST /sync/costs
    """
    try:
        result = await data_sync.sync_cost_sheet()
        return result
    except Exception as e:
        log.error(f"Cost sheet sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shopify")
async def sync_shopify(
    days: int = Query(30, description="Number of days to sync"),
    include_products: bool = Query(True, description="Include products (set to false for faster sync)")
):
    """
    Sync Shopify data

    Set include_products=false for faster orders-only sync (~5 sec vs ~2 min)
    """
    try:
        result = await data_sync.sync_shopify(days=days, include_products=include_products)
        clear_cache()
        return result
    except Exception as e:
        log.error(f"Shopify sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shopify/quick")
async def sync_shopify_quick(days: int = Query(0, description="Number of days to sync (0=today)")):
    """
    Quick Shopify sync - orders only, no products (~5 seconds)

    Use this for chat/real-time queries. Skips the 35K+ product catalog.
    """
    try:
        result = await data_sync.sync_shopify(days=days, include_products=False)
        return result
    except Exception as e:
        log.error(f"Shopify quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shopify/backfill")
async def backfill_shopify(
    days: int = Query(365, description="Days of history to backfill (default 365)", ge=1, le=730)
):
    """
    Backfill Shopify historical data.

    Fetches and saves:
    - Products with full variant details (including SKU, vendor)
    - Customers (all)
    - Orders (within date range)
    - Refunds (for refunded/partially_refunded orders)
    - Inventory snapshot (current quantities)

    Creates DataSyncLog entry for tracking.

    - **days**: How far back to backfill orders (max 730 = 2 years)

    Example: POST /sync/shopify/backfill?days=365
    """
    try:
        result = await data_sync.backfill_shopify(days=days)
        return result
    except Exception as e:
        log.error(f"Shopify backfill error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shopify/inventory")
async def sync_shopify_inventory():
    """
    Sync current Shopify inventory snapshot.

    Fetches inventory levels for all variants, including:
    - SKU
    - Current quantity
    - Cost (if available)
    - Inventory policy

    Creates DataSyncLog entry for tracking.

    Example: POST /sync/shopify/inventory
    """
    try:
        result = await data_sync.sync_shopify_inventory()
        return result
    except Exception as e:
        log.error(f"Shopify inventory sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shopify/stats")
async def get_shopify_stats():
    """
    Get current Shopify data statistics.

    Returns counts and date ranges for products, customers, orders, refunds, inventory.
    Use to verify backfill completeness.
    """
    from sqlalchemy import func
    from app.models.shopify import ShopifyProduct, ShopifyCustomer, ShopifyOrder, ShopifyRefund, ShopifyInventory

    db = SessionLocal()
    try:
        stats = {}

        # Products
        product_count = db.query(func.count(ShopifyProduct.id)).scalar() or 0
        product_with_sku = db.query(func.count(ShopifyProduct.id)).filter(
            ShopifyProduct.variants.isnot(None)
        ).scalar() or 0
        stats['products'] = {
            'count': product_count,
            'with_variants': product_with_sku,
        }

        # Customers
        customer_count = db.query(func.count(ShopifyCustomer.id)).scalar() or 0
        customer_dates = db.query(
            func.min(ShopifyCustomer.created_at),
            func.max(ShopifyCustomer.created_at)
        ).first()
        stats['customers'] = {
            'count': customer_count,
            'earliest': customer_dates[0].isoformat() if customer_dates[0] else None,
            'latest': customer_dates[1].isoformat() if customer_dates[1] else None,
        }

        # Orders
        order_count = db.query(func.count(ShopifyOrder.id)).scalar() or 0
        order_dates = db.query(
            func.min(ShopifyOrder.created_at),
            func.max(ShopifyOrder.created_at)
        ).first()
        stats['orders'] = {
            'count': order_count,
            'earliest': order_dates[0].isoformat() if order_dates[0] else None,
            'latest': order_dates[1].isoformat() if order_dates[1] else None,
        }

        # Refunds
        refund_count = db.query(func.count(ShopifyRefund.id)).scalar() or 0
        refund_dates = db.query(
            func.min(ShopifyRefund.created_at),
            func.max(ShopifyRefund.created_at)
        ).first()
        stats['refunds'] = {
            'count': refund_count,
            'earliest': refund_dates[0].isoformat() if refund_dates[0] else None,
            'latest': refund_dates[1].isoformat() if refund_dates[1] else None,
        }

        # Inventory (active products only)
        active_pids = db.query(ShopifyProduct.shopify_product_id).filter(
            ShopifyProduct.status == 'active'
        ).subquery()
        inv_base = db.query(func.count(ShopifyInventory.id)).filter(
            ShopifyInventory.shopify_product_id.in_(active_pids)
        )
        inventory_count = inv_base.scalar() or 0
        inventory_with_sku = inv_base.filter(
            ShopifyInventory.sku.isnot(None),
            ShopifyInventory.sku != ''
        ).scalar() or 0
        stats['inventory'] = {
            'count': inventory_count,
            'with_sku': inventory_with_sku,
        }

        return stats

    except Exception as e:
        log.error(f"Get Shopify stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/klaviyo")
async def sync_klaviyo(days: int = Query(30, description="Number of days to sync")):
    """
    Sync Klaviyo data
    """
    try:
        result = await data_sync.sync_klaviyo(days=days)
        return result
    except Exception as e:
        log.error(f"Klaviyo sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ga4")
async def sync_ga4(days: int = Query(30, description="Number of days to sync")):
    """
    Sync Google Analytics 4 data
    """
    try:
        result = await data_sync.sync_ga4(days=days)
        clear_cache()
        return result
    except Exception as e:
        log.error(f"GA4 sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/google-ads")
async def sync_google_ads(days: int = Query(30, description="Number of days to sync")):
    """
    Sync Google Ads data
    """
    try:
        result = await data_sync.sync_google_ads(days=days)
        return result
    except Exception as e:
        log.error(f"Google Ads sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/merchant-center")
async def sync_merchant_center(quick: bool = Query(False, description="Quick mode - skip full product list")):
    """
    Sync Google Merchant Center data

    - Full sync: Gets all products, statuses, and account info
    - Quick mode (quick=true): Skip full product list, just statuses and summary
    """
    try:
        result = await data_sync.sync_merchant_center(quick=quick)
        return result
    except Exception as e:
        log.error(f"Merchant Center sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/merchant-center/quick")
async def sync_merchant_center_quick():
    """
    Quick Merchant Center sync - statuses only, no full product list

    Use this for chat/real-time queries.
    """
    try:
        result = await data_sync.sync_merchant_center(quick=True)
        return result
    except Exception as e:
        log.error(f"Merchant Center quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shippit")
async def sync_shippit(
    days: int = Query(7, description="Number of days of fulfilled orders to check"),
):
    """
    Sync Shippit shipping cost data.

    Looks up fulfilled Shopify orders in Shippit to get actual shipping costs.
    """
    try:
        result = await data_sync.sync_shippit(days=days)
        return result
    except Exception as e:
        log.error(f"Shippit sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shippit/backfill")
async def backfill_shippit(
    days: int = Query(90, description="Days of history to backfill"),
):
    """Backfill Shippit shipping costs for historical orders."""
    try:
        result = await data_sync.sync_shippit(days=days)
        return result
    except Exception as e:
        log.error(f"Shippit backfill error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/github")
async def sync_github(
    days: int = Query(7, description="Days of commit history"),
    quick: bool = Query(False, description="Quick mode - skip file contents")
):
    """
    Sync GitHub repository data (Shopify theme)

    - Full sync: Gets commits, branches, PRs, and critical file info
    - Quick mode (quick=true): Skip file contents for faster sync
    """
    try:
        result = await data_sync.sync_github(days=days, quick=quick)
        return result
    except Exception as e:
        log.error(f"GitHub sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/github/quick")
async def sync_github_quick():
    """
    Quick GitHub sync - repo info, recent commits, and PRs only

    Use this for chat/real-time queries.
    """
    try:
        result = await data_sync.sync_github(quick=True)
        return result
    except Exception as e:
        log.error(f"GitHub quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search-console")
async def sync_search_console(
    days: int = Query(480, description="Days to sync (max 480 = 16 months)"),
    quick: bool = Query(False, description="Quick mode - last 7 days only")
):
    """
    Sync Google Search Console data

    - Full sync: Gets queries, pages, devices, countries (up to 16 months)
    - Quick mode (quick=true): Last 7 days summary only
    """
    try:
        result = await data_sync.sync_search_console(days=days, quick=quick)
        clear_cache()
        return result
    except Exception as e:
        log.error(f"Search Console sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search-console/quick")
async def sync_search_console_quick():
    """
    Quick Search Console sync - last 7 days summary

    Use this for chat/real-time queries.
    """
    try:
        result = await data_sync.sync_search_console(quick=True)
        return result
    except Exception as e:
        log.error(f"Search Console quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search-console/backfill")
async def backfill_search_console(
    months: int = Query(16, description="Months to backfill (max 16)", ge=1, le=16),
    window_days: int = Query(14, description="Days per fetch window (7-30)", ge=7, le=30),
    delay: float = Query(2.0, description="Seconds between windows (rate limit protection)", ge=0, le=10)
):
    """
    Backfill Search Console historical data.

    Fetches data in chunks to avoid rate limits. Progress is logged per window.
    Errors in individual windows don't stop the entire backfill.

    - **months**: How far back to backfill (Search Console max is 16 months)
    - **window_days**: Size of each fetch window (smaller = more API calls, larger = risk of timeouts)
    - **delay**: Pause between windows to respect rate limits

    Example: POST /sync/search-console/backfill?months=16&window_days=14
    """
    try:
        result = await data_sync.backfill_search_console(
            months=months,
            window_days=window_days,
            delay_between_windows=delay
        )
        return result
    except Exception as e:
        log.error(f"Search Console backfill error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search-console/daily")
async def daily_sync_search_console(
    days: int = Query(3, description="Days to sync (1-7)", ge=1, le=7)
):
    """
    Daily incremental Search Console sync.

    Syncs only recent data (accounts for Search Console's 2-3 day data delay).
    Use this for scheduled daily syncs.

    - **days**: Number of recent days to sync (default 3)

    Example: POST /sync/search-console/daily?days=3
    """
    try:
        result = await data_sync.daily_sync_search_console(days=days)
        return result
    except Exception as e:
        log.error(f"Search Console daily sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_sync_status():
    """
    Get sync status for all connectors
    """
    try:
        status = data_sync.get_sync_status()
        return status
    except Exception as e:
        log.error(f"Get sync status error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_sync_logs(
    source: Optional[str] = Query(None, description="Filter by source (shopify, klaviyo, ga4, etc.)"),
    status: Optional[str] = Query(None, description="Filter by status (success, failed, partial)"),
    hours: int = Query(24, description="Hours of history to retrieve"),
    limit: int = Query(100, description="Maximum number of logs to return")
):
    """
    Get sync history logs.

    Use this to audit sync operations, track failures, and verify data freshness.
    """
    db = SessionLocal()
    try:
        query = db.query(DataSyncLog)

        # Apply filters
        if source:
            query = query.filter(DataSyncLog.source == source)
        if status:
            query = query.filter(DataSyncLog.status == status)

        # Time filter
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        query = query.filter(DataSyncLog.started_at >= cutoff)

        # Order and limit
        logs = query.order_by(DataSyncLog.started_at.desc()).limit(limit).all()

        return {
            "count": len(logs),
            "filters": {
                "source": source,
                "status": status,
                "hours": hours
            },
            "logs": [
                {
                    "id": entry.id,
                    "source": entry.source,
                    "sync_type": entry.sync_type,
                    "status": entry.status,
                    "records_processed": entry.records_processed,
                    "records_created": entry.records_created,
                    "records_updated": entry.records_updated,
                    "records_failed": entry.records_failed,
                    "error_message": entry.error_message,
                    "duration_seconds": entry.duration_seconds,
                    "started_at": entry.started_at.isoformat() if entry.started_at else None,
                    "completed_at": entry.completed_at.isoformat() if entry.completed_at else None
                }
                for entry in logs
            ]
        }
    except Exception as e:
        log.error(f"Get sync logs error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/logs/summary")
async def get_sync_logs_summary(hours: int = Query(24, description="Hours of history to analyze")):
    """
    Get summary statistics of sync operations.

    Useful for monitoring dashboards and health checks.
    """
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        logs = db.query(DataSyncLog).filter(DataSyncLog.started_at >= cutoff).all()

        # Group by source
        by_source = {}
        for log_entry in logs:
            source = log_entry.source
            if source not in by_source:
                by_source[source] = {
                    "total_syncs": 0,
                    "successful": 0,
                    "failed": 0,
                    "partial": 0,
                    "total_records_created": 0,
                    "total_records_updated": 0,
                    "total_records_failed": 0,
                    "avg_duration_seconds": 0,
                    "last_sync": None,
                    "last_status": None
                }

            stats = by_source[source]
            stats["total_syncs"] += 1
            stats["total_records_created"] += log_entry.records_created or 0
            stats["total_records_updated"] += log_entry.records_updated or 0
            stats["total_records_failed"] += log_entry.records_failed or 0

            if log_entry.status == "success":
                stats["successful"] += 1
            elif log_entry.status == "failed":
                stats["failed"] += 1
            elif log_entry.status == "partial":
                stats["partial"] += 1

            # Track latest
            if stats["last_sync"] is None or (log_entry.started_at and log_entry.started_at.isoformat() > stats["last_sync"]):
                stats["last_sync"] = log_entry.started_at.isoformat() if log_entry.started_at else None
                stats["last_status"] = log_entry.status

        # Calculate averages
        for source, stats in by_source.items():
            source_logs = [l for l in logs if l.source == source and l.duration_seconds]
            if source_logs:
                stats["avg_duration_seconds"] = round(
                    sum(l.duration_seconds for l in source_logs) / len(source_logs), 2
                )

        # Overall stats
        total_syncs = len(logs)
        success_rate = (
            sum(1 for l in logs if l.status == "success") / total_syncs * 100
            if total_syncs > 0 else 0
        )

        return {
            "period_hours": hours,
            "total_syncs": total_syncs,
            "success_rate_percent": round(success_rate, 1),
            "by_source": by_source
        }
    except Exception as e:
        log.error(f"Get sync logs summary error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ── Caprice Pricing Import ──────────────────────────────────────────


@router.post("/caprice/import-latest")
def import_caprice_latest():
    """
    Manually trigger Caprice pricing file import.

    Scans /imports/new-sheets for unprocessed .xlsx files,
    imports them into competitive_pricing, and moves files
    to processed/ or failed/.

    Runs synchronously (in FastAPI threadpool) because it
    uses pandas + DB writes.
    """
    try:
        service = CapriceImportService()
        result = service.run_import()
        return {"success": True, **result}
    except Exception as e:
        log.error(f"Caprice import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/caprice/log")
def get_caprice_import_log(
    status: Optional[str] = Query(None, description="Filter by status (success, failed, skipped)"),
    limit: int = Query(50, description="Max entries"),
):
    """
    Get Caprice import history from the log table.
    """
    db = SessionLocal()
    try:
        query = db.query(CapriceImportLog).order_by(CapriceImportLog.imported_at.desc())
        if status:
            query = query.filter(CapriceImportLog.status == status)
        entries = query.limit(limit).all()

        return {
            "count": len(entries),
            "logs": [
                {
                    "id": e.id,
                    "filename": e.filename,
                    "checksum": e.checksum[:12] + "…",
                    "status": e.status,
                    "rows_imported": e.rows_imported,
                    "rows_updated": e.rows_updated,
                    "pricing_date": e.pricing_date,
                    "error": e.error,
                    "imported_at": e.imported_at.isoformat() if e.imported_at else None,
                }
                for e in entries
            ],
        }
    except Exception as e:
        log.error(f"Get Caprice log error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ── Google Ads CSV Import ──────────────────────────────────────────


@router.post("/google-ads/import-csv")
def import_google_ads_csv():
    """
    Import Google Ads data from CSV files exported from the Google Ads web UI.

    Drop .csv files into imports/google-ads/new/ then call this endpoint.
    The service auto-detects the report type (campaigns, ad_groups, products,
    search_terms), maps columns, and upserts into the google_ads_* tables.
    Files are moved to processed/ or failed/ after import.
    """
    try:
        service = GoogleAdsImportService()
        result = service.run_import()
        return {"success": True, **result}
    except Exception as e:
        log.error(f"Google Ads CSV import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/google-ads/import-log")
def get_google_ads_import_log(
    status: Optional[str] = Query(None, description="Filter by status (success, failed, skipped)"),
    csv_type: Optional[str] = Query(None, description="Filter by type (campaigns, ad_groups, products, search_terms)"),
    limit: int = Query(50, description="Max entries"),
):
    """
    Get Google Ads CSV import history.
    """
    db = SessionLocal()
    try:
        query = db.query(GoogleAdsImportLog).order_by(GoogleAdsImportLog.imported_at.desc())
        if status:
            query = query.filter(GoogleAdsImportLog.status == status)
        if csv_type:
            query = query.filter(GoogleAdsImportLog.csv_type == csv_type)
        entries = query.limit(limit).all()

        return {
            "count": len(entries),
            "logs": [
                {
                    "id": e.id,
                    "filename": e.filename,
                    "checksum": e.checksum[:12] + "...",
                    "status": e.status,
                    "csv_type": e.csv_type,
                    "rows_imported": e.rows_imported,
                    "rows_updated": e.rows_updated,
                    "rows_skipped": e.rows_skipped,
                    "rows_errored": e.rows_errored,
                    "date_range": e.date_range,
                    "error": e.error,
                    "imported_at": e.imported_at.isoformat() if e.imported_at else None,
                }
                for e in entries
            ],
        }
    except Exception as e:
        log.error(f"Get Google Ads import log error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ── Google Ads Sheet Import (automated via Google Ads Scripts) ────


@router.post("/google-ads/import-from-sheet")
def import_google_ads_from_sheet(
    sheet_id: Optional[str] = Query(None, description="Google Sheets spreadsheet ID (uses config default if omitted)"),
    tab: Optional[str] = Query(None, description="Campaign tab name (default: 'Campaign Data')"),
):
    """
    Import Google Ads campaign AND product data from a Google Sheet.

    Reads two tabs:
    - "Campaign Data" — campaign-level daily metrics → google_ads_campaigns
    - "Product Data"  — product/SKU-level daily metrics → google_ads_products

    The ads manager sets up a Google Ads Script that writes both tabs daily.
    """
    from app.config import get_settings

    try:
        settings = get_settings()

        resolved_sheet_id = sheet_id or settings.google_ads_sheet_id
        if not resolved_sheet_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No sheet_id provided and GOOGLE_ADS_SHEET_ID is not set in .env. "
                    "Pass ?sheet_id=YOUR_SHEET_ID or set the env var."
                ),
            )

        credentials_path = settings.google_sheets_credentials_path
        resolved_tab = tab or settings.google_ads_sheet_tab

        service = GoogleAdsSheetImportService()

        # Import campaigns
        campaign_result = service.import_from_sheet(
            sheet_id=resolved_sheet_id,
            credentials_path=credentials_path,
            tab_name=resolved_tab,
        )

        # Import products (non-blocking — tab may not exist yet)
        product_result = service.import_products_from_sheet(
            sheet_id=resolved_sheet_id,
            credentials_path=credentials_path,
            tab_name="Product Data",
        )

        if not campaign_result.get("success"):
            raise HTTPException(status_code=500, detail=campaign_result.get("error", "Unknown error"))

        return {
            "success": True,
            "campaigns": campaign_result,
            "products": product_result,
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Google Ads Sheet import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/google-ads/sheet-status")
def get_google_ads_sheet_status():
    """
    Check Google Ads Sheet configuration and connectivity.

    Returns whether GOOGLE_ADS_SHEET_ID is configured and, if credentials
    are available, reads the _metadata tab to show when data was last
    exported by the Google Ads Script.
    """
    from app.config import get_settings

    try:
        settings = get_settings()
        status = {
            "sheet_id_configured": bool(settings.google_ads_sheet_id),
            "sheet_id": settings.google_ads_sheet_id or "(not set)",
            "tab_name": settings.google_ads_sheet_tab,
            "credentials_path": settings.google_sheets_credentials_path,
        }

        if not settings.google_ads_sheet_id:
            status["message"] = "Set GOOGLE_ADS_SHEET_ID in .env to enable Sheet import"
            return status

        # Try to read metadata
        if GOOGLE_AVAILABLE:
            try:
                from google.oauth2 import service_account as sa
                from googleapiclient.discovery import build as build_svc

                creds = sa.Credentials.from_service_account_file(
                    settings.google_sheets_credentials_path,
                    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
                )
                svc = build_svc("sheets", "v4", credentials=creds)

                # Read metadata tab (created by Google Ads Script, may not exist yet)
                try:
                    meta = svc.spreadsheets().values().get(
                        spreadsheetId=settings.google_ads_sheet_id,
                        range="'_metadata'!A:B",
                    ).execute()
                    meta_rows = meta.get("values", [])
                    for row in meta_rows:
                        if len(row) >= 2:
                            status[row[0].strip().lower().replace(" ", "_")] = row[1]
                except Exception:
                    status["metadata_tab"] = "not found (Google Ads Script hasn't run yet)"

                # Read row count from main tab
                try:
                    data = svc.spreadsheets().values().get(
                        spreadsheetId=settings.google_ads_sheet_id,
                        range=f"'{settings.google_ads_sheet_tab}'!A:A",
                    ).execute()
                    data_rows = data.get("values", [])
                    status["data_rows"] = max(0, len(data_rows) - 1)
                except Exception:
                    status["data_rows"] = 0
                    status["data_tab"] = f"tab '{settings.google_ads_sheet_tab}' not found"

                status["connected"] = True
            except Exception as e:
                status["connected"] = False
                status["connection_error"] = str(e)
        else:
            status["connected"] = False
            status["connection_error"] = "Google API libraries not installed"

        return status

    except Exception as e:
        log.error(f"Google Ads Sheet status error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
