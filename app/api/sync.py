"""
Data synchronization endpoints
"""
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, UploadFile, File
from typing import Optional, List
from datetime import datetime, timedelta
from app.models.base import SessionLocal
from app.models.analytics import DataSyncLog
from app.models.caprice_import import CapriceImportLog
from app.models.google_ads_import import GoogleAdsImportLog
from app.utils.logger import log
from app.utils.cache import clear_cache, clear_for_source
from app.utils.response_cache import response_cache

router = APIRouter(prefix="/sync", tags=["sync"])

# In-memory sync status for background tasks
_sync_status = {}


def _update_sync_status(source: str, status: str, result=None, error=None):
    _sync_status[source] = {
        "status": status,
        "started_at": _sync_status.get(source, {}).get("started_at", datetime.utcnow().isoformat()),
        "updated_at": datetime.utcnow().isoformat(),
        "result": result,
        "error": error,
    }

# Lazy-init to avoid loading heavy connectors (shopify/GA4) at startup
_data_sync = None

def _get_data_sync():
    global _data_sync
    if _data_sync is None:
        from app.services.data_sync_service import DataSyncService
        _data_sync = DataSyncService()
    return _data_sync



async def _run_sync_all(days: int):
    """Background task: sync all sources."""
    _update_sync_status("all", "running")
    try:
        result = await _get_data_sync().sync_all(days=days)
        clear_cache()
        response_cache.clear()
        _update_sync_status("all", "completed", result=result)
        log.info(f"Background sync_all completed: {result.get('sources_synced', '?')}/{result.get('total_sources', '?')} sources")
    except Exception as e:
        log.error(f"Background sync_all error: {str(e)}")
        _update_sync_status("all", "failed", error=str(e))


@router.post("/all")
async def sync_all_sources(
    background_tasks: BackgroundTasks,
    days: int = Query(30, description="Number of days to sync"),
):
    """
    Sync data from all sources (runs in background to avoid timeout).
    Check progress at GET /sync/progress
    """
    _update_sync_status("all", "started")
    background_tasks.add_task(_run_sync_all, days)
    return {
        "message": "Sync started in background",
        "sources": ["shopify", "klaviyo", "ga4", "search_console", "merchant_center"],
        "days": days,
        "check_progress": "/sync/progress",
    }


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
        result = await _get_data_sync().sync_cost_sheet()
        return result
    except Exception as e:
        log.error(f"Cost sheet sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _run_sync_shopify(days: int, include_products: bool):
    """Background task: sync Shopify."""
    _update_sync_status("shopify", "running")
    try:
        result = await _get_data_sync().sync_shopify(days=days, include_products=include_products)
        clear_for_source("shopify")
        response_cache.invalidate("profitability:")
        response_cache.invalidate("customers:")
        response_cache.invalidate("monitor:")
        _update_sync_status("shopify", "completed", result=result)
        log.info(f"Background sync_shopify completed")
    except Exception as e:
        log.error(f"Background sync_shopify error: {str(e)}")
        _update_sync_status("shopify", "failed", error=str(e))


@router.post("/shopify")
async def sync_shopify(
    background_tasks: BackgroundTasks,
    days: int = Query(30, description="Number of days to sync"),
    include_products: bool = Query(True, description="Include products (set to false for faster sync)")
):
    """
    Sync Shopify data (runs in background).
    Check progress at GET /sync/progress
    """
    _update_sync_status("shopify", "started")
    background_tasks.add_task(_run_sync_shopify, days, include_products)
    return {"message": "Shopify sync started in background", "days": days, "check_progress": "/sync/progress"}


@router.post("/shopify/quick")
async def sync_shopify_quick(days: int = Query(0, description="Number of days to sync (0=today)")):
    """
    Quick Shopify sync - orders only, no products (~5 seconds)

    Use this for chat/real-time queries. Skips the 35K+ product catalog.
    """
    try:
        result = await _get_data_sync().sync_shopify(days=days, include_products=False)
        return result
    except Exception as e:
        log.error(f"Shopify quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _run_backfill_shopify(days: int):
    _update_sync_status("shopify_backfill", "running")
    try:
        result = await _get_data_sync().backfill_shopify(days=days)
        clear_for_source("shopify")
        _update_sync_status("shopify_backfill", "completed", result=result)
    except Exception as e:
        log.error(f"Background shopify backfill error: {str(e)}")
        _update_sync_status("shopify_backfill", "failed", error=str(e))


@router.post("/shopify/backfill")
async def backfill_shopify(
    background_tasks: BackgroundTasks,
    days: int = Query(365, description="Days of history to backfill (default 365)", ge=1, le=730)
):
    """Backfill Shopify history (background). Check GET /sync/progress"""
    _update_sync_status("shopify_backfill", "started")
    background_tasks.add_task(_run_backfill_shopify, days)
    return {"message": f"Shopify backfill started in background ({days} days)", "check_progress": "/sync/progress"}


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
        result = await _get_data_sync().sync_shopify_inventory()
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


async def _run_sync_klaviyo(days: int):
    _update_sync_status("klaviyo", "running")
    try:
        result = await _get_data_sync().sync_klaviyo(days=days)
        _update_sync_status("klaviyo", "completed", result=result)
    except Exception as e:
        log.error(f"Background sync_klaviyo error: {str(e)}")
        _update_sync_status("klaviyo", "failed", error=str(e))


@router.post("/klaviyo")
async def sync_klaviyo(background_tasks: BackgroundTasks, days: int = Query(30, description="Number of days to sync")):
    """Sync Klaviyo data (background). Check GET /sync/progress"""
    _update_sync_status("klaviyo", "started")
    background_tasks.add_task(_run_sync_klaviyo, days)
    return {"message": "Klaviyo sync started in background", "check_progress": "/sync/progress"}


async def _run_sync_ga4(days: int):
    _update_sync_status("ga4", "running")
    try:
        result = await _get_data_sync().sync_ga4(days=days)
        clear_for_source("ga4")
        response_cache.invalidate("monitor:")
        _update_sync_status("ga4", "completed", result=result)
    except Exception as e:
        log.error(f"Background sync_ga4 error: {str(e)}")
        _update_sync_status("ga4", "failed", error=str(e))


@router.post("/ga4")
async def sync_ga4(background_tasks: BackgroundTasks, days: int = Query(30, description="Number of days to sync")):
    """Sync GA4 data (background). Check GET /sync/progress"""
    _update_sync_status("ga4", "started")
    background_tasks.add_task(_run_sync_ga4, days)
    return {"message": "GA4 sync started in background", "check_progress": "/sync/progress"}


@router.post("/google-ads")
async def sync_google_ads(days: int = Query(30, description="Number of days to sync")):
    """
    Sync Google Ads data
    """
    try:
        result = await _get_data_sync().sync_google_ads(days=days)
        return result
    except Exception as e:
        log.error(f"Google Ads sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _run_sync_merchant_center(quick: bool):
    _update_sync_status("merchant_center", "running")
    try:
        result = await _get_data_sync().sync_merchant_center(quick=quick)
        _update_sync_status("merchant_center", "completed", result=result)
    except Exception as e:
        log.error(f"Background sync_merchant_center error: {str(e)}")
        _update_sync_status("merchant_center", "failed", error=str(e))


@router.post("/merchant-center")
async def sync_merchant_center(background_tasks: BackgroundTasks, quick: bool = Query(False, description="Quick mode - skip full product list")):
    """Sync Merchant Center data (background). Check GET /sync/progress"""
    _update_sync_status("merchant_center", "started")
    background_tasks.add_task(_run_sync_merchant_center, quick)
    return {"message": "Merchant Center sync started in background", "check_progress": "/sync/progress"}


@router.post("/merchant-center/quick")
async def sync_merchant_center_quick():
    """
    Quick Merchant Center sync - statuses only, no full product list

    Use this for chat/real-time queries.
    """
    try:
        result = await _get_data_sync().sync_merchant_center(quick=True)
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
        result = await _get_data_sync().sync_shippit(days=days)
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
        result = await _get_data_sync().sync_shippit(days=days)
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
        result = await _get_data_sync().sync_github(days=days, quick=quick)
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
        result = await _get_data_sync().sync_github(quick=True)
        return result
    except Exception as e:
        log.error(f"GitHub quick sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _run_sync_search_console(days: int, quick: bool):
    _update_sync_status("search_console", "running")
    try:
        result = await _get_data_sync().sync_search_console(days=days, quick=quick)
        clear_for_source("search_console")
        response_cache.invalidate("monitor:")
        _update_sync_status("search_console", "completed", result=result)
    except Exception as e:
        log.error(f"Background sync_search_console error: {str(e)}")
        _update_sync_status("search_console", "failed", error=str(e))


@router.post("/search-console")
async def sync_search_console(
    background_tasks: BackgroundTasks,
    days: int = Query(480, description="Days to sync (max 480 = 16 months)"),
    quick: bool = Query(False, description="Quick mode - last 7 days only")
):
    """Sync Search Console data (background). Check GET /sync/progress"""
    _update_sync_status("search_console", "started")
    background_tasks.add_task(_run_sync_search_console, days, quick)
    return {"message": "Search Console sync started in background", "check_progress": "/sync/progress"}


@router.post("/search-console/quick")
async def sync_search_console_quick():
    """
    Quick Search Console sync - last 7 days summary

    Use this for chat/real-time queries.
    """
    try:
        result = await _get_data_sync().sync_search_console(quick=True)
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
        result = await _get_data_sync().backfill_search_console(
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
        result = await _get_data_sync().daily_sync_search_console(days=days)
        return result
    except Exception as e:
        log.error(f"Search Console daily sync error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diagnose")
async def diagnose_connectors():
    """
    Diagnose connector configuration — checks env vars, credential files,
    and attempts validation for each source.  Returns details to help
    identify why syncs fail.
    """
    import os
    import traceback
    from app.config import get_settings

    settings = get_settings()
    results = {}

    # Helper: mask a value (show first 4 chars)
    def _mask(val: str) -> str:
        if not val:
            return "(empty)"
        return val[:4] + "****" if len(val) > 4 else "****"

    # Raw-test helper: bypass connector's internal try/except to get real errors
    async def _raw_test(name, test_fn):
        """Run test_fn and return (success, error_detail)."""
        try:
            result = await test_fn() if asyncio.iscoroutinefunction(test_fn) else test_fn()
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {str(e)}"

    import asyncio, aiohttp, shopify as shopify_lib

    # 1. Shopify — call the API directly to see the real error
    try:
        results["shopify"] = {
            "shop_url": settings.shopify_shop_url or "(empty)",
            "api_version": settings.shopify_api_version,
            "access_token": _mask(settings.shopify_access_token),
            "api_key": _mask(settings.shopify_api_key),
        }
        session = shopify_lib.Session(
            settings.shopify_shop_url,
            settings.shopify_api_version,
            settings.shopify_access_token,
        )
        shopify_lib.ShopifyResource.activate_session(session)
        try:
            shop = shopify_lib.Shop.current()
            results["shopify"]["connection_valid"] = shop is not None
            if shop:
                results["shopify"]["shop_name"] = shop.name
        except Exception as e:
            results["shopify"]["connection_valid"] = False
            results["shopify"]["connection_error"] = f"{type(e).__name__}: {str(e)}"
    except Exception as e:
        results["shopify"] = {"init_error": f"{type(e).__name__}: {str(e)}"}

    # 2. Klaviyo — hit the accounts endpoint directly
    try:
        results["klaviyo"] = {
            "api_key": _mask(settings.klaviyo_api_key),
        }
        async with aiohttp.ClientSession() as http:
            async with http.get(
                "https://a.klaviyo.com/api/accounts/",
                headers={
                    "Authorization": f"Klaviyo-API-Key {settings.klaviyo_api_key}",
                    "revision": "2024-02-15",
                    "Accept": "application/json",
                },
            ) as resp:
                results["klaviyo"]["http_status"] = resp.status
                results["klaviyo"]["connection_valid"] = resp.status == 200
                if resp.status != 200:
                    body = await resp.text()
                    results["klaviyo"]["connection_error"] = body[:500]
    except Exception as e:
        results["klaviyo"]["connection_valid"] = False
        results["klaviyo"]["connection_error"] = f"{type(e).__name__}: {str(e)}"

    # 3. GA4 — load credentials and make a test request
    try:
        cred_path = settings.ga4_credentials_path
        results["ga4"] = {
            "property_id": settings.ga4_property_id or "(empty)",
            "credentials_path": cred_path,
            "credentials_file_exists": os.path.exists(cred_path),
        }
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            cred_path, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        client = BetaAnalyticsDataClient(credentials=creds)
        request = RunReportRequest(
            property=f"properties/{settings.ga4_property_id}",
            date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
            metrics=[Metric(name="activeUsers")],
        )
        client.run_report(request)
        results["ga4"]["connection_valid"] = True
    except Exception as e:
        results["ga4"]["connection_valid"] = False
        results["ga4"]["connection_error"] = f"{type(e).__name__}: {str(e)}"

    # 4. Google Ads
    try:
        results["google_ads"] = {
            "customer_id": _mask(settings.google_ads_customer_id),
            "developer_token": _mask(settings.google_ads_developer_token),
            "client_id": _mask(settings.google_ads_client_id),
            "refresh_token": _mask(settings.google_ads_refresh_token),
        }
        from app.connectors.google_ads_connector import GoogleAdsConnector
        conn = GoogleAdsConnector()
        await conn.connect()
        ga_svc = conn.client.get_service("GoogleAdsService")
        ga_svc.search(customer_id=settings.google_ads_customer_id, query="SELECT customer.id FROM customer LIMIT 1")
        results["google_ads"]["connection_valid"] = True
    except Exception as e:
        results["google_ads"]["connection_valid"] = False
        results["google_ads"]["connection_error"] = f"{type(e).__name__}: {str(e)}"

    # 5. Merchant Center
    try:
        mc_cred_path = settings.merchant_center_credentials_path
        results["merchant_center"] = {
            "merchant_id": settings.merchant_center_id or "(empty)",
            "credentials_path": mc_cred_path,
            "credentials_file_exists": os.path.exists(mc_cred_path),
        }
        from google.oauth2 import service_account as mc_sa
        from googleapiclient.discovery import build as mc_build
        mc_creds = mc_sa.Credentials.from_service_account_file(
            mc_cred_path, scopes=["https://www.googleapis.com/auth/content"]
        )
        mc_svc = mc_build("content", "v2.1", credentials=mc_creds)
        mc_result = mc_svc.accounts().get(
            merchantId=settings.merchant_center_id,
            accountId=settings.merchant_center_id,
        ).execute()
        results["merchant_center"]["connection_valid"] = True
        results["merchant_center"]["account_name"] = mc_result.get("name", "?")
    except Exception as e:
        results["merchant_center"]["connection_valid"] = False
        results["merchant_center"]["connection_error"] = f"{type(e).__name__}: {str(e)}"

    # 6. Credential files check
    cred_dir = "./credentials"
    results["credential_files"] = {
        "directory_exists": os.path.isdir(cred_dir),
        "files": os.listdir(cred_dir) if os.path.isdir(cred_dir) else [],
    }

    # 7. GOOGLE_SA_JSON env var check
    sa_val = os.environ.get("GOOGLE_SA_JSON", "")
    results["google_sa_json_env"] = {
        "is_set": bool(sa_val),
        "looks_like_json": sa_val.strip().startswith("{") if sa_val else False,
        "length": len(sa_val),
    }

    return results


@router.get("/progress")
async def get_sync_progress():
    """Get progress of background sync tasks."""
    return _sync_status or {"message": "No syncs have been started yet"}


@router.get("/status")
async def get_sync_status():
    """
    Get sync status for all connectors
    """
    try:
        status = _get_data_sync().get_sync_status()
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


@router.post("/caprice/upload")
async def upload_caprice_file(file: UploadFile = File(...)):
    """
    Upload a Caprice pricing .xlsx file and import it synchronously
    using fast bulk operations (completes within Render's 30s timeout).
    """
    import os
    import hashlib
    import tempfile
    import re
    from datetime import date as date_type

    import pandas as pd
    from sqlalchemy import text

    if not file.filename or not file.filename.endswith(".xlsx"):
        raise HTTPException(400, "File must be an .xlsx Excel file")

    db = SessionLocal()
    tmp_path = None
    try:
        # Save to temp file (pandas needs a file path)
        contents = await file.read()
        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, file.filename)
        with open(tmp_path, "wb") as f:
            f.write(contents)

        log.info(f"Caprice upload: saved {file.filename} ({len(contents)} bytes)")

        # Checksum for dedup
        checksum = hashlib.sha256(contents).hexdigest()

        already = db.query(CapriceImportLog).filter(
            CapriceImportLog.checksum == checksum,
            CapriceImportLog.status == "success",
        ).first()
        if already:
            return {"success": True, "uploaded": file.filename,
                    "message": "Already imported (duplicate file)", "rows_imported": 0}

        # Extract date from filename (DDMMYYYY)
        match = re.search(r'(\d{2})(\d{2})(\d{4})', file.filename)
        if match:
            d, m, y = match.groups()
            pricing_date = date_type(int(y), int(m), int(d))
        else:
            pricing_date = date_type.today()

        # Detect sheet and read Excel
        xl = pd.ExcelFile(tmp_path)
        if 'Prices Today' in xl.sheet_names:
            sheet_name = 'Prices Today'
        elif 'log' in xl.sheet_names:
            sheet_name = 'log'
        else:
            sheet_name = xl.sheet_names[0]

        df = pd.read_excel(tmp_path, sheet_name=sheet_name)
        total_rows = len(df)
        log.info(f"Caprice upload: {file.filename} has {total_rows} rows, sheet '{sheet_name}'")

        # Column mapping (Excel column -> DB column)
        col_map = {
            'Variant ID': 'variant_id', 'variantId': 'variant_id',
            'Match': 'match_rule', 'Set Price': 'set_price',
            'Ceiling Price': 'ceiling_price', 'Vendor': 'vendor',
            'Variant SKU': 'variant_sku', 'Title': 'title',
            'RRP': 'rrp', '% Off': 'discount_off_rrp_pct',
            'Current Cass Price': 'current_price', 'Cass Minimum': 'minimum_price',
            'Lowest Price': 'lowest_competitor_price',
            'LowestPrice-MinPrice': 'price_vs_minimum',
            '$ Below Minimum': 'price_vs_minimum',
            'NETT': 'nett_cost', '% Profit Margin': 'profit_margin_pct',
            'Profit': 'profit_amount', 'Profit ($)': 'profit_amount',
            '8appliances': 'price_8appliances',
            'appliancesonline': 'price_appliancesonline',
            'austpek': 'price_austpek', 'binglee': 'price_binglee',
            'blueleafbath': 'price_blueleafbath',
            'brandsdirectonline': 'price_brandsdirect',
            'buildmat': 'price_buildmat', 'cookandbathe': 'price_cookandbathe',
            'designerbathware': 'price_designerbathware',
            'harveynorman': 'price_harveynorman',
            'idealbathroomcentre': 'price_idealbathroom',
            'justbathroomware': 'price_justbathroomware',
            'thebluespace': 'price_thebluespace', 'wellsons': 'price_wellsons',
            'winnings': 'price_winnings', 'agcequipment': 'price_agcequipment',
            'berloniappliances': 'price_berloniapp', 'eands': 'price_eands',
            'plumbingsales': 'price_plumbingsales', 'powerland': 'price_powerland',
            'saappliancewarehouse': 'price_saappliances',
            'samedayhotwaterservice': 'price_sameday',
            'shireskylights': 'price_shire', 'voguespas': 'price_vogue',
        }

        # Determine variant ID column
        vid_col = 'Variant ID' if 'Variant ID' in df.columns else 'variantId'
        if vid_col not in df.columns:
            raise HTTPException(400, "No Variant ID column found in file")

        # Rename columns to DB field names (drop unmapped columns)
        rename = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename)
        db_cols = list(rename.values())
        df = df[db_cols].copy()

        # Drop rows without variant_id, deduplicate
        df = df.dropna(subset=['variant_id'])
        df['variant_id'] = df['variant_id'].astype(int)
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['variant_id'], keep='first')
        skipped = total_rows - len(df)

        # Add metadata columns
        now = datetime.utcnow()
        df['pricing_date'] = pricing_date
        df['source_file'] = file.filename
        df['import_date'] = now

        if df.empty:
            raise HTTPException(400, "No valid rows found in file")

        # Delete existing rows for this pricing_date using raw SQL (fast)
        from app.models.base import engine
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM competitive_pricing WHERE pricing_date = :d"),
                {"d": str(pricing_date)}
            )
            deleted = result.rowcount

        log.info(f"Caprice upload: deleted {deleted} existing rows for {pricing_date}")

        # Bulk insert using pandas to_sql (works reliably for both SQLite and PostgreSQL)
        from app.models.base import engine
        rows_inserted = len(df)

        # Debug: capture column info for troubleshooting
        competitor_cols_in_df = [c for c in df.columns if c.startswith('price_')]
        competitor_non_null = {c: int(df[c].notna().sum()) for c in competitor_cols_in_df}
        debug_info = {
            "total_df_columns": len(df.columns),
            "df_columns": list(df.columns),
            "competitor_columns_count": len(competitor_cols_in_df),
            "competitor_non_null_counts": competitor_non_null,
        }
        log.info(f"Caprice upload: {len(competitor_cols_in_df)} competitor cols, "
                 f"non-null counts: {competitor_non_null}")

        # Replace NaN/NaT with None for proper NULL handling
        df = df.where(df.notna(), None)

        df.to_sql(
            'competitive_pricing', engine, if_exists='append',
            index=False, chunksize=500, method='multi'
        )

        log.info(f"Caprice upload: inserted {rows_inserted} rows for {pricing_date}")

        # Log success
        db.add(CapriceImportLog(
            filename=file.filename,
            checksum=checksum,
            status="success",
            rows_imported=rows_inserted,
            rows_updated=0,
            rows_skipped=skipped,
            pricing_date=str(pricing_date),
            imported_at=now,
        ))
        db.commit()

        # Quick verification: count rows with competitor data
        verify_count = 0
        verify_sample = []
        try:
            from sqlalchemy import text as sql_text
            with engine.connect() as vconn:
                res = vconn.execute(sql_text(
                    "SELECT COUNT(*) FROM competitive_pricing "
                    "WHERE pricing_date = :d AND price_8appliances IS NOT NULL"
                ), {"d": str(pricing_date)})
                verify_count = res.scalar()

                res2 = vconn.execute(sql_text(
                    "SELECT variant_sku, price_8appliances, price_buildmat "
                    "FROM competitive_pricing "
                    "WHERE pricing_date = :d AND price_8appliances IS NOT NULL LIMIT 3"
                ), {"d": str(pricing_date)})
                verify_sample = [dict(r._mapping) for r in res2]
        except Exception as ve:
            log.error(f"Verify error: {ve}")

        return {
            "success": True,
            "uploaded": file.filename,
            "pricing_date": str(pricing_date),
            "rows_imported": rows_inserted,
            "rows_deleted": deleted,
            "rows_skipped": skipped,
            "verify_with_competitors": verify_count,
            "verify_samples": verify_sample,
            "debug": debug_info,
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Caprice upload error: {str(e)}")
        # Log failure
        try:
            db.add(CapriceImportLog(
                filename=file.filename,
                checksum=hashlib.sha256(contents).hexdigest() if contents else "error",
                status="failed",
                error=str(e)[:500],
                imported_at=datetime.utcnow(),
            ))
            db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
        if tmp_path:
            try:
                os.unlink(tmp_path)
                os.rmdir(os.path.dirname(tmp_path))
            except Exception:
                pass


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
        from app.services.caprice_import_service import CapriceImportService
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


@router.get("/caprice/diagnose")
def diagnose_caprice_data():
    """
    Diagnostic endpoint — show competitor data status for ALL pricing dates.
    """
    from sqlalchemy import text
    from app.models.base import engine

    results = {"code_version": "to_sql_v2"}
    try:
        with engine.connect() as conn:
            # Get all pricing dates with their row counts and competitor counts
            rows = conn.execute(text(
                "SELECT pricing_date, COUNT(*) as total, "
                "SUM(CASE WHEN price_8appliances IS NOT NULL THEN 1 ELSE 0 END) as with_8app, "
                "SUM(CASE WHEN price_buildmat IS NOT NULL THEN 1 ELSE 0 END) as with_buildmat, "
                "SUM(CASE WHEN price_harveynorman IS NOT NULL THEN 1 ELSE 0 END) as with_harvey "
                "FROM competitive_pricing "
                "GROUP BY pricing_date ORDER BY pricing_date DESC"
            )).fetchall()

            results["dates"] = [
                {
                    "pricing_date": str(r[0]),
                    "total_rows": r[1],
                    "with_8appliances": r[2],
                    "with_buildmat": r[3],
                    "with_harveynorman": r[4],
                }
                for r in rows
            ]

            # Sample a row with competitor data from the most recent date that has any
            for r in rows:
                if r[2] > 0:  # has 8appliances data
                    sample = conn.execute(text(
                        "SELECT variant_sku, rrp, current_price, price_8appliances, "
                        "price_buildmat, price_harveynorman "
                        "FROM competitive_pricing "
                        "WHERE pricing_date = :d AND price_8appliances IS NOT NULL LIMIT 3"
                    ), {"d": str(r[0])}).fetchall()
                    results["sample_with_competitors"] = [
                        dict(s._mapping) for s in sample
                    ]
                    break

        return results

    except Exception as e:
        log.error(f"Diagnose error: {str(e)}")
        return {"error": str(e), "partial_results": results}


@router.delete("/caprice/date/{pricing_date}")
def delete_caprice_date(pricing_date: str):
    """Delete all competitive_pricing rows for a specific date (to remove broken imports)."""
    from sqlalchemy import text
    from app.models.base import engine

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM competitive_pricing WHERE pricing_date = :d"),
            {"d": pricing_date}
        )
        return {"deleted_rows": result.rowcount, "pricing_date": pricing_date}


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
        from app.services.google_ads_import_service import GoogleAdsImportService
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

        from app.services.google_ads_sheet_import import GoogleAdsSheetImportService
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

        # Update DataSyncStatus so freshness checks see google_ads as current
        try:
            from app.models.data_quality import DataSyncStatus
            from app.models.base import SessionLocal
            _db = SessionLocal()
            status_row = _db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name == 'google_ads'
            ).first()
            if status_row:
                status_row.last_successful_sync = datetime.utcnow()
                status_row.records_synced = campaign_result.get('rows_imported', 0)
            else:
                status_row = DataSyncStatus(
                    source_name='google_ads',
                    last_successful_sync=datetime.utcnow(),
                    records_synced=campaign_result.get('rows_imported', 0),
                )
                _db.add(status_row)
            _db.commit()
            _db.close()
        except Exception as status_err:
            log.warning(f"Failed to update google_ads sync status: {status_err}")

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
        from app.services.google_ads_sheet_import import GOOGLE_AVAILABLE
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
