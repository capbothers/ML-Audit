"""
Scheduler for automated data connector syncs

Uses APScheduler to run connector syncs at optimal intervals.
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import asyncio
from typing import Optional
import time

# Lazy imports: connectors and heavy services are imported inside job functions
# to avoid loading everything at startup (saves ~50MB on Render free tier).
from app.config import get_settings
from app.utils.logger import log

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

settings = get_settings()
scheduler = AsyncIOScheduler()
_stale_recovery_lock = asyncio.Lock()

# Global sync lock — only one sync job runs at a time on Render free tier (512MB).
# Without this, overlapping syncs stack memory and cause OOM crashes.
_sync_lock = asyncio.Lock()


async def _guarded_sync(name: str, coro_fn):
    """Run a sync function under the global lock with gc.collect() after."""
    import gc
    if _sync_lock.locked():
        log.info(f"Skipping {name} — another sync is already running")
        return
    async with _sync_lock:
        log.info(f"[sync-guard] Starting {name}")
        try:
            await coro_fn()
        finally:
            gc.collect()
            log.info(f"[sync-guard] Finished {name}, memory freed")


def _extract_sync_counts(result: dict) -> tuple:
    """Extract (records_created, records_updated) from a connector result dict.

    Old-style connectors return only 'records_synced'.  If a connector adds
    'records_created'/'records_updated' in the future, those will be used instead.
    """
    created = result.get('records_created', 0)
    updated = result.get('records_updated', 0)
    synced = result.get('records_synced', 0)
    # If neither specific key was set, attribute everything to created
    if created == 0 and updated == 0 and synced > 0:
        created = synced
    return created, updated


# Sync Functions

async def sync_google_ads():
    """Sync Google Ads data (hourly)"""
    from app.connectors.google_ads import GoogleAdsConnector
    from app.services.data_sync_service import SyncResult, update_data_sync_status
    from app.models.base import get_db
    start = time.time()
    try:
        log.info("Starting Google Ads sync...")
        db = next(get_db())

        connector = GoogleAdsConnector(db)
        await connector.authenticate()
        result = await connector.sync()

        if result['success']:
            log.info(f"Google Ads sync completed: {result['records_synced']} records in {result['duration_seconds']:.1f}s")
            created, updated = _extract_sync_counts(result)
            try:
                update_data_sync_status(SyncResult(
                    source="google_ads", sync_type="incremental", status="success",
                    records_created=created, records_updated=updated,
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass
        else:
            log.error(f"Google Ads sync failed: {result.get('error')}")
            try:
                update_data_sync_status(SyncResult(
                    source="google_ads", sync_type="incremental", status="failed",
                    error_message=result.get('error'),
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass

        db.close()

    except Exception as e:
        log.error(f"Google Ads sync error: {str(e)}")
        try:
            update_data_sync_status(SyncResult(
                source="google_ads", sync_type="incremental", status="failed",
                error_message=str(e),
                started_at=datetime.utcfromtimestamp(start),
                completed_at=datetime.utcnow(),
                duration_seconds=time.time() - start,
            ))
        except Exception:
            pass


async def sync_stale_connectors():
    """
    Catch-up guardrail for critical sources if regular schedule windows were missed.

    This is especially important when the app sleeps/restarts and cron windows pass.
    """
    from app.models.base import SessionLocal
    from app.models.data_quality import DataSyncStatus

    if _stale_recovery_lock.locked():
        log.info("Stale connector recovery already running, skipping overlapping run")
        return

    # GA4 excluded from stale recovery — it's too memory-heavy for catch-up
    # on Render free tier (512MB). GA4 will still sync on its regular cron
    # schedule (4am + 4pm AEST).
    thresholds_hours = {
        "shopify": 12,
        "search_console": 120,
        "merchant_center": 72,
        "google_ads": 72,
    }

    google_ads_sync_fn = sync_google_ads_sheet if settings.google_ads_sheet_id else sync_google_ads
    sync_map = {
        "shopify": sync_shopify,
        "search_console": sync_search_console,
        "merchant_center": sync_merchant_center,
        "google_ads": google_ads_sync_fn,
    }

    async with _stale_recovery_lock:
        import gc

        db = SessionLocal()
        try:
            statuses = db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name.in_(list(thresholds_hours.keys()))
            ).all()
            by_source = {s.source_name: s for s in statuses}
        finally:
            db.close()

        now = datetime.utcnow()
        stale_sources = []
        for source, threshold in thresholds_hours.items():
            status = by_source.get(source)
            last_success = status.last_successful_sync if status else None
            lag_hours = ((now - last_success).total_seconds() / 3600.0) if last_success else None

            if lag_hours is not None and lag_hours <= threshold:
                continue
            stale_sources.append(source)

        if not stale_sources:
            return

        log.warning(f"Stale recovery: {len(stale_sources)} sources need catch-up: {stale_sources}")

        for source in stale_sources:
            lag_display = "never synced"
            status = by_source.get(source)
            if status and status.last_successful_sync:
                lag_hours = (now - status.last_successful_sync).total_seconds() / 3600.0
                lag_display = f"{lag_hours:.1f}h stale"

            log.warning(f"Stale recovery: triggering {source} sync ({lag_display})")
            try:
                await sync_map[source]()
            except Exception as e:
                log.error(f"Stale recovery failed for {source}: {str(e)}")

            # Free memory between syncs to avoid OOM on Render free tier
            gc.collect()
            await asyncio.sleep(5)


async def sync_ga4():
    """Sync GA4 data (twice daily) using DataSyncService"""
    from app.services.data_sync_service import DataSyncService
    try:
        log.info("Starting GA4 sync...")
        sync_service = DataSyncService()
        # GA4 has 24-48h delay, sync last 3 days (keeps memory low on Render free tier)
        result = await sync_service.sync_ga4(days=3)

        if result.get('success'):
            log.info(
                f"GA4 sync completed: "
                f"{result.get('traffic_overview_saved', 0)} overview, "
                f"{result.get('traffic_sources_saved', 0)} sources, "
                f"{result.get('pages_saved', 0)} pages, "
                f"{result.get('landing_pages_saved', 0)} landing pages, "
                f"{result.get('products_saved', 0)} products, "
                f"{result.get('events_saved', 0)} events, "
                f"{result.get('ecommerce_saved', 0)} ecommerce in {result.get('duration', 0):.1f}s"
            )
        else:
            log.error(f"GA4 sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"GA4 sync error: {str(e)}")


async def sync_search_console():
    """Sync Search Console data (daily at 5am) using DataSyncService"""
    from app.services.data_sync_service import DataSyncService
    try:
        log.info("Starting Search Console daily sync...")
        sync_service = DataSyncService()
        result = await sync_service.daily_sync_search_console(days=3)

        if result.get('success'):
            log.info(
                f"Search Console sync completed: "
                f"{result.get('queries_saved', 0)} queries, "
                f"{result.get('pages_saved', 0)} pages, "
                f"{result.get('sitemaps_saved', 0)} sitemaps in {result.get('duration_seconds', 0):.1f}s"
            )
        else:
            log.error(f"Search Console sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Search Console sync error: {str(e)}")


async def sync_klaviyo():
    """Sync Klaviyo data (hourly)"""
    from app.connectors.klaviyo import KlaviyoConnector
    from app.services.data_sync_service import SyncResult, update_data_sync_status
    from app.models.base import get_db
    start = time.time()
    try:
        log.info("Starting Klaviyo sync...")
        db = next(get_db())

        connector = KlaviyoConnector(db)
        await connector.authenticate()
        result = await connector.sync()

        if result['success']:
            log.info(f"Klaviyo sync completed: {result['records_synced']} records in {result['duration_seconds']:.1f}s")
            created, updated = _extract_sync_counts(result)
            try:
                update_data_sync_status(SyncResult(
                    source="klaviyo", sync_type="incremental", status="success",
                    records_created=created, records_updated=updated,
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass
        else:
            log.error(f"Klaviyo sync failed: {result.get('error')}")
            try:
                update_data_sync_status(SyncResult(
                    source="klaviyo", sync_type="incremental", status="failed",
                    error_message=result.get('error'),
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass

        db.close()

    except Exception as e:
        log.error(f"Klaviyo sync error: {str(e)}")
        try:
            update_data_sync_status(SyncResult(
                source="klaviyo", sync_type="incremental", status="failed",
                error_message=str(e),
                started_at=datetime.utcfromtimestamp(start),
                completed_at=datetime.utcnow(),
                duration_seconds=time.time() - start,
            ))
        except Exception:
            pass


async def sync_hotjar():
    """Sync Hotjar/Clarity data (daily at 6am)"""
    from app.connectors.hotjar import HotjarConnector
    from app.services.data_sync_service import SyncResult, update_data_sync_status
    from app.models.base import get_db
    start = time.time()
    try:
        log.info("Starting Hotjar/Clarity sync...")
        db = next(get_db())

        connector = HotjarConnector(db)
        await connector.authenticate()
        result = await connector.sync()

        if result['success']:
            log.info(f"Hotjar/Clarity sync completed: {result['records_synced']} records in {result['duration_seconds']:.1f}s")
            created, updated = _extract_sync_counts(result)
            try:
                update_data_sync_status(SyncResult(
                    source="hotjar", sync_type="incremental", status="success",
                    records_created=created, records_updated=updated,
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass
        else:
            log.error(f"Hotjar/Clarity sync failed: {result.get('error')}")
            try:
                update_data_sync_status(SyncResult(
                    source="hotjar", sync_type="incremental", status="failed",
                    error_message=result.get('error'),
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass

        db.close()

    except Exception as e:
        log.error(f"Hotjar/Clarity sync error: {str(e)}")
        try:
            update_data_sync_status(SyncResult(
                source="hotjar", sync_type="incremental", status="failed",
                error_message=str(e),
                started_at=datetime.utcfromtimestamp(start),
                completed_at=datetime.utcnow(),
                duration_seconds=time.time() - start,
            ))
        except Exception:
            pass


async def sync_github():
    """Sync GitHub data (daily at 7am)"""
    from app.connectors.github import GitHubConnector
    from app.services.data_sync_service import SyncResult, update_data_sync_status
    from app.models.base import get_db
    start = time.time()
    try:
        log.info("Starting GitHub sync...")
        db = next(get_db())

        connector = GitHubConnector(db)
        await connector.authenticate()
        result = await connector.sync()

        if result['success']:
            log.info(f"GitHub sync completed: {result['records_synced']} records in {result['duration_seconds']:.1f}s")
            created, updated = _extract_sync_counts(result)
            try:
                update_data_sync_status(SyncResult(
                    source="github", sync_type="incremental", status="success",
                    records_created=created, records_updated=updated,
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass
        else:
            log.error(f"GitHub sync failed: {result.get('error')}")
            try:
                update_data_sync_status(SyncResult(
                    source="github", sync_type="incremental", status="failed",
                    error_message=result.get('error'),
                    started_at=datetime.utcfromtimestamp(start),
                    completed_at=datetime.utcnow(),
                    duration_seconds=time.time() - start,
                ))
            except Exception:
                pass

        db.close()

    except Exception as e:
        log.error(f"GitHub sync error: {str(e)}")
        try:
            update_data_sync_status(SyncResult(
                source="github", sync_type="incremental", status="failed",
                error_message=str(e),
                started_at=datetime.utcfromtimestamp(start),
                completed_at=datetime.utcnow(),
                duration_seconds=time.time() - start,
            ))
        except Exception:
            pass


async def run_ml_intelligence():
    """Run ML intelligence pipeline (daily at 3am)"""
    from app.services.ml_intelligence_service import MLIntelligenceService
    from app.models.base import get_db
    try:
        log.info("Starting ML intelligence pipeline...")
        db = next(get_db())

        service = MLIntelligenceService(db)
        result = service.run_daily_ml_pipeline()

        forecasts = result.get('forecasts', {})
        anomalies = result.get('anomalies', {})
        inventory = result.get('inventory', {})

        log.info(
            f"ML pipeline completed: "
            f"{forecasts.get('forecasts_generated', 0)} forecasts, "
            f"{anomalies.get('anomalies_upserted', 0)} anomalies, "
            f"{inventory.get('total_skus_analyzed', 0)} inventory SKUs "
            f"({inventory.get('critical_count', 0)} critical)"
        )

        db.close()

    except Exception as e:
        log.error(f"ML intelligence pipeline error: {str(e)}")


async def sync_shopify():
    """Sync Shopify orders and order items (every 2 hours)"""
    from app.services.data_sync_service import DataSyncService
    try:
        log.info("Starting Shopify sync...")
        sync_service = DataSyncService()
        # Sync last 3 days to catch delayed updates and ensure order_items are current
        result = await sync_service.sync_shopify(days=3, include_products=False)

        if result.get('success'):
            log.info(
                f"Shopify sync completed: "
                f"{result.get('orders_saved', 0)} new, "
                f"{result.get('orders_updated', 0)} updated in {result.get('duration', 0):.1f}s"
            )
        else:
            log.error(f"Shopify sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Shopify sync error: {str(e)}")


async def sync_merchant_center():
    """Sync Merchant Center data (daily at 2am)"""
    from app.services.data_sync_service import DataSyncService
    try:
        log.info("Starting Merchant Center sync...")
        sync_service = DataSyncService()
        result = await sync_service.sync_merchant_center(quick=False)

        if result.get('success'):
            log.info(
                f"Merchant Center sync completed: "
                f"{result.get('statuses_saved', 0)} statuses, "
                f"{result.get('disapprovals_saved', 0)} disapprovals in {result.get('duration', 0):.1f}s"
            )
        else:
            log.error(f"Merchant Center sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Merchant Center sync error: {str(e)}")


async def sync_shippit():
    """Sync Shippit shipping costs (every 6 hours)"""
    from app.services.data_sync_service import DataSyncService
    try:
        log.info("Starting Shippit sync...")
        sync_service = DataSyncService()
        result = await sync_service.sync_shippit(days=7)

        if result.get('success'):
            log.info(
                f"Shippit sync completed: "
                f"{result.get('orders_saved', 0)} new, "
                f"{result.get('orders_updated', 0)} updated in {result.get('duration', 0):.1f}s"
            )
        else:
            log.error(f"Shippit sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Shippit sync error: {str(e)}")


async def sync_caprice_pricing():
    """Import new Caprice pricing files (daily at 1pm)"""
    from app.services.caprice_import_service import CapriceImportService
    try:
        log.info("Starting Caprice pricing import...")
        service = CapriceImportService()
        result = service.run_import()

        imported = result.get('imported', 0)
        skipped = result.get('skipped', 0)
        failed = result.get('failed', 0)
        total_rows = result.get('total_rows', 0)

        log.info(
            f"Caprice import completed: {imported} files imported "
            f"({total_rows} rows), {skipped} skipped, {failed} failed"
        )

    except Exception as e:
        log.error(f"Caprice pricing import error: {str(e)}")


async def sync_competitor_blogs():
    """Scrape competitor/supplier blogs (daily at 7:30am AEST)"""
    from app.services.competitor_blog_service import CompetitorBlogService
    try:
        log.info("Starting competitor blog scrape...")
        service = CompetitorBlogService()
        result = await service.sync_competitor_blogs(days=30)

        if result.get('success'):
            log.info(
                f"Competitor blog scrape completed: "
                f"{result.get('new_articles', 0)} new, "
                f"{result.get('updated_articles', 0)} updated from "
                f"{result.get('sites_scraped', 0)} sites in {result.get('duration', 0):.1f}s"
            )
        else:
            log.error(f"Competitor blog scrape failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Competitor blog scrape error: {str(e)}")


async def sync_shopify_full():
    """Full Shopify sync including products (daily at 1am AEST)"""
    from app.services.data_sync_service import DataSyncService
    from app.utils.cache import clear_for_source
    from app.utils.response_cache import response_cache
    try:
        log.info("Starting Shopify full sync (with products)...")
        sync_service = DataSyncService()
        result = await sync_service.sync_shopify(days=3, include_products=True)

        if result.get('success'):
            log.info(
                f"Shopify full sync completed: "
                f"{result.get('orders_saved', 0)} orders, "
                f"{result.get('products_saved', 0)} products in {result.get('duration', 0):.1f}s"
            )
            clear_for_source("shopify")
            response_cache.invalidate("profitability:")
            response_cache.invalidate("customers:")
            response_cache.invalidate("monitor:")
        else:
            log.error(f"Shopify full sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Shopify full sync error: {str(e)}")


async def sync_google_ads_sheet():
    """Import Google Ads data from Google Sheet (daily at 6am AEST)"""
    from app.services.google_ads_sheet_import import GoogleAdsSheetImportService
    from app.utils.cache import clear_for_source
    from app.utils.response_cache import response_cache
    try:
        log.info("Starting Google Ads Sheet import...")
        settings = get_settings()

        if not settings.google_ads_sheet_id:
            log.info("GOOGLE_ADS_SHEET_ID not set, skipping Google Ads Sheet import")
            return

        service = GoogleAdsSheetImportService()
        campaign_result = service.import_from_sheet(
            sheet_id=settings.google_ads_sheet_id,
            credentials_path=settings.google_sheets_credentials_path,
            tab_name=settings.google_ads_sheet_tab,
        )
        product_result = service.import_products_from_sheet(
            sheet_id=settings.google_ads_sheet_id,
            credentials_path=settings.google_sheets_credentials_path,
            tab_name="Product Data",
        )

        log.info(
            f"Google Ads Sheet import completed: "
            f"campaigns={campaign_result.get('rows_imported', 0)}, "
            f"products={product_result.get('rows_imported', 0)}"
        )
        clear_for_source("google_ads")
        response_cache.invalidate("ads:")
        response_cache.invalidate("monitor:")

    except Exception as e:
        log.error(f"Google Ads Sheet import error: {str(e)}")


async def sync_cost_sheet():
    """Sync NETT Master cost sheet from Google Sheets (daily at 4:30am AEST)"""
    from app.services.data_sync_service import DataSyncService
    from app.utils.cache import clear_for_source
    from app.utils.response_cache import response_cache
    try:
        log.info("Starting cost sheet sync...")
        sync_service = DataSyncService()
        result = await sync_service.sync_cost_sheet()

        if result.get('success'):
            log.info(
                f"Cost sheet sync completed: "
                f"{result.get('vendors_synced', 0)} vendors, "
                f"{result.get('products_synced', 0)} products in {result.get('duration', 0):.1f}s"
            )
            clear_for_source("cost_sheet")
            response_cache.invalidate("profitability:")
            response_cache.invalidate("monitor:")
        else:
            log.error(f"Cost sheet sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Cost sheet sync error: {str(e)}")


async def score_decision_outcomes_7d():
    """Score 7-day decision outcomes (daily at 4am)"""
    from app.models.base import get_db
    try:
        log.info("Scoring 7-day decision outcomes...")
        db = next(get_db())
        from app.services.decision_feedback import DecisionFeedbackService
        svc = DecisionFeedbackService(db)
        scored = svc.score_outcomes(7)
        log.info(f"Scored {scored} decision outcomes (7d)")
        db.close()
    except Exception as e:
        log.error(f"Decision outcome scoring (7d) error: {str(e)}")


async def score_decision_outcomes_30d():
    """Score 30-day decision outcomes (daily at 4:15am)"""
    from app.models.base import get_db
    try:
        log.info("Scoring 30-day decision outcomes...")
        db = next(get_db())
        from app.services.decision_feedback import DecisionFeedbackService
        svc = DecisionFeedbackService(db)
        scored = svc.score_outcomes(30)
        log.info(f"Scored {scored} decision outcomes (30d)")
        db.close()
    except Exception as e:
        log.error(f"Decision outcome scoring (30d) error: {str(e)}")


# ── Guarded wrappers — scheduler calls these, not the raw sync functions ──
# Each wrapper acquires the global _sync_lock so only one sync runs at a time.

async def _g_shopify():        await _guarded_sync("shopify", sync_shopify)
async def _g_shopify_full():   await _guarded_sync("shopify_full", sync_shopify_full)
async def _g_ga4():            await _guarded_sync("ga4", sync_ga4)
async def _g_search_console(): await _guarded_sync("search_console", sync_search_console)
async def _g_merchant_center():await _guarded_sync("merchant_center", sync_merchant_center)
async def _g_klaviyo():        await _guarded_sync("klaviyo", sync_klaviyo)
async def _g_hotjar():         await _guarded_sync("hotjar", sync_hotjar)
async def _g_github():         await _guarded_sync("github", sync_github)
async def _g_ml():             await _guarded_sync("ml_intelligence", run_ml_intelligence)
async def _g_caprice():        await _guarded_sync("caprice", sync_caprice_pricing)
async def _g_shippit():        await _guarded_sync("shippit", sync_shippit)
async def _g_blogs():          await _guarded_sync("competitor_blogs", sync_competitor_blogs)
async def _g_cost_sheet():     await _guarded_sync("cost_sheet", sync_cost_sheet)
async def _g_google_ads():     await _guarded_sync("google_ads", sync_google_ads)
async def _g_google_ads_sheet(): await _guarded_sync("google_ads_sheet", sync_google_ads_sheet)
async def _g_score_7d():       await _guarded_sync("score_7d", score_decision_outcomes_7d)
async def _g_score_30d():      await _guarded_sync("score_30d", score_decision_outcomes_30d)


# Schedule Configuration

def setup_scheduler():
    """
    Configure and start the scheduler.

    All cron times are Australia/Sydney (AEST/AEDT).

    Sync Frequencies:
    - Shopify orders:     Every 2 hours (orders + order_items for COGS/P&L)
    - Shopify full:       Daily 1:00am  (includes products/variants catalog)
    - Google Ads (Sheet): Daily 6:00am  (campaign + product data from Google Sheets)
      OR Google Ads API:  Every hour    (when sheet mode is not configured)
    - Cost Sheet (NETT):  Daily 4:30am  (product costs from Google Sheets)
    - GA4:                4:00am + 4:00pm (5-day window covers data delay)
    - Search Console:     Daily 5:00am  (has 2-3 day delay)
    - Merchant Center:    Daily 2:00am  (product feed health)
    - Klaviyo:            Every hour    (campaign performance)
    - Hotjar/Clarity:     Daily 6:30am  (behavior data)
    - GitHub:             Daily 7:00am  (theme commits)
    - ML Intelligence:    Daily 3:00am  (after overnight syncs)
    - Caprice Pricing:    Daily 1:00pm  (pricing file import)
    """

    # ── Memory-safe scheduling for Render free tier (512MB) ──
    #
    # Key design decisions:
    # 1. All jobs use _g_* guarded wrappers — global lock ensures only ONE sync at a time
    # 2. Cron jobs: misfire_grace_time=NONE (0) — if the window was missed, skip it and
    #    wait for the next scheduled run. This prevents ALL daily jobs from firing at once
    #    after a restart/sleep.
    # 3. Interval jobs: deferred start (next_run_time) so they don't fire immediately on boot
    # 4. Stale recovery handles catch-up for critical sources (shopify, search_console, etc.)
    #
    now = datetime.now(SYDNEY_TZ)

    # ── Shopify ──────────────────────────────────────────
    # Orders only - every 2 hours (first run 10min after startup)
    scheduler.add_job(
        _g_shopify,
        trigger=IntervalTrigger(hours=2),
        id='shopify_sync',
        name='Shopify Orders & Items Sync',
        replace_existing=True,
        max_instances=1,
        next_run_time=now + timedelta(minutes=10),
    )
    # Full sync (with products) - daily at 1am AEST
    scheduler.add_job(
        _g_shopify_full,
        trigger=CronTrigger(hour=1, minute=0, timezone=SYDNEY_TZ),
        id='shopify_full_sync',
        name='Shopify Full Sync (with products)',
        replace_existing=True,
        max_instances=1,
    )

    # ── Google Ads ───────────────────────────────────────
    if settings.google_ads_sheet_id:
        # Sheet import - daily at 6am AEST
        scheduler.add_job(
            _g_google_ads_sheet,
            trigger=CronTrigger(hour=6, minute=0, timezone=SYDNEY_TZ),
            id='google_ads_sheet_sync',
            name='Google Ads Sheet Daily Import',
            replace_existing=True,
            max_instances=1,
        )
    else:
        # API sync - every 2 hours (first run 20min after startup)
        scheduler.add_job(
            _g_google_ads,
            trigger=IntervalTrigger(hours=2),
            id='google_ads_api_sync',
            name='Google Ads API Sync',
            replace_existing=True,
            max_instances=1,
            next_run_time=now + timedelta(minutes=20),
        )

    # ── Cost Sheet (NETT Master) ─────────────────────────
    # Daily at 4:30am AEST
    scheduler.add_job(
        _g_cost_sheet,
        trigger=CronTrigger(hour=4, minute=30, timezone=SYDNEY_TZ),
        id='cost_sheet_sync',
        name='Cost Sheet Daily Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── GA4 ──────────────────────────────────────────────
    # Twice daily: 4am and 4pm AEST
    scheduler.add_job(
        _g_ga4,
        trigger=CronTrigger(hour=4, minute=0, timezone=SYDNEY_TZ),
        id='ga4_sync_morning',
        name='GA4 Morning Sync',
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        _g_ga4,
        trigger=CronTrigger(hour=16, minute=0, timezone=SYDNEY_TZ),
        id='ga4_sync_afternoon',
        name='GA4 Afternoon Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── Search Console ───────────────────────────────────
    # Daily at 5am AEST
    scheduler.add_job(
        _g_search_console,
        trigger=CronTrigger(hour=5, minute=0, timezone=SYDNEY_TZ),
        id='search_console_sync',
        name='Search Console Daily Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── Merchant Center ──────────────────────────────────
    # Daily at 2am AEST
    scheduler.add_job(
        _g_merchant_center,
        trigger=CronTrigger(hour=2, minute=0, timezone=SYDNEY_TZ),
        id='merchant_center_sync',
        name='Merchant Center Daily Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── Klaviyo ──────────────────────────────────────────
    # Every 2 hours (first run 30min after startup)
    scheduler.add_job(
        _g_klaviyo,
        trigger=IntervalTrigger(hours=2),
        id='klaviyo_sync',
        name='Klaviyo Sync',
        replace_existing=True,
        max_instances=1,
        next_run_time=now + timedelta(minutes=30),
    )

    # ── Hotjar/Clarity ───────────────────────────────────
    # Daily at 6:30am AEST
    scheduler.add_job(
        _g_hotjar,
        trigger=CronTrigger(hour=6, minute=30, timezone=SYDNEY_TZ),
        id='hotjar_sync',
        name='Hotjar/Clarity Daily Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── GitHub ───────────────────────────────────────────
    # Daily at 7am AEST
    scheduler.add_job(
        _g_github,
        trigger=CronTrigger(hour=7, minute=0, timezone=SYDNEY_TZ),
        id='github_sync',
        name='GitHub Daily Sync',
        replace_existing=True,
        max_instances=1,
    )

    # ── ML Intelligence ──────────────────────────────────
    # Daily at 3am AEST (after overnight data syncs complete)
    scheduler.add_job(
        _g_ml,
        trigger=CronTrigger(hour=3, minute=0, timezone=SYDNEY_TZ),
        id='ml_intelligence',
        name='ML Intelligence Daily Pipeline',
        replace_existing=True,
        max_instances=1,
    )

    # ── Caprice Pricing ──────────────────────────────────
    # Daily at 1pm AEST
    scheduler.add_job(
        _g_caprice,
        trigger=CronTrigger(hour=13, minute=0, timezone=SYDNEY_TZ),
        id='caprice_pricing_import',
        name='Caprice Pricing Daily Import',
        replace_existing=True,
        max_instances=1,
    )

    # ── Shippit ────────────────────────────────────────────
    # Every 6 hours (first run 40min after startup)
    if settings.shippit_api_key:
        scheduler.add_job(
            _g_shippit,
            trigger=IntervalTrigger(hours=6),
            id='shippit_sync',
            name='Shippit Shipping Cost Sync',
            replace_existing=True,
            max_instances=1,
            next_run_time=now + timedelta(minutes=40),
        )

    # ── Competitor Blogs ─────────────────────────────────
    # Daily at 7:30am AEST (after main syncs, before work day)
    scheduler.add_job(
        _g_blogs,
        trigger=CronTrigger(hour=7, minute=30, timezone=SYDNEY_TZ),
        id='competitor_blogs_sync',
        name='Competitor Blog Daily Scrape',
        replace_existing=True,
        max_instances=1,
    )

    # ── Decision Outcome Scoring ──────────────────────────
    # Daily at 4am AEST: score 7-day outcomes
    scheduler.add_job(
        _g_score_7d,
        trigger=CronTrigger(hour=4, minute=0, timezone=SYDNEY_TZ),
        id='decision_outcomes_7d',
        name='Score 7-Day Decision Outcomes',
        replace_existing=True,
        max_instances=1,
    )
    # Daily at 4:15am AEST: score 30-day outcomes
    scheduler.add_job(
        _g_score_30d,
        trigger=CronTrigger(hour=4, minute=15, timezone=SYDNEY_TZ),
        id='decision_outcomes_30d',
        name='Score 30-Day Decision Outcomes',
        replace_existing=True,
        max_instances=1,
    )

    # ── Catch-up Guardrail ───────────────────────────────
    # Every 4 hours, detect stale critical sources and trigger catch-up syncs.
    # First run 20 minutes after startup to let the app stabilize.
    scheduler.add_job(
        sync_stale_connectors,
        trigger=IntervalTrigger(hours=4),
        id='stale_recovery_sync',
        name='Stale Connector Recovery',
        replace_existing=True,
        max_instances=1,
        next_run_time=now + timedelta(minutes=20),
    )

    log.info("Scheduler configured — all jobs use global sync lock (one-at-a-time)")


def start_scheduler():
    """Start the scheduler"""
    setup_scheduler()
    scheduler.start()
    log.info("Scheduler started — stale recovery will run in 15 minutes")


def stop_scheduler():
    """Stop the scheduler"""
    scheduler.shutdown()
    log.info("Scheduler stopped")


def run_sync_now(connector_name: str) -> dict:
    """
    Manually trigger a sync for a specific connector

    Args:
        connector_name: Name of connector (google_ads, ga4, search_console, klaviyo, hotjar, github)

    Returns:
        Dict with sync results
    """
    sync_functions = {
        'shopify': sync_shopify,
        'shopify_full': sync_shopify_full,
        'google_ads': sync_google_ads,
        'google_ads_sheet': sync_google_ads_sheet,
        'cost_sheet': sync_cost_sheet,
        'ga4': sync_ga4,
        'search_console': sync_search_console,
        'merchant_center': sync_merchant_center,
        'klaviyo': sync_klaviyo,
        'hotjar': sync_hotjar,
        'github': sync_github,
        'caprice': sync_caprice_pricing,
        'shippit': sync_shippit,
        'competitor_blogs': sync_competitor_blogs,
    }

    if connector_name not in sync_functions:
        return {
            'success': False,
            'error': f'Unknown connector: {connector_name}. Valid options: {", ".join(sync_functions.keys())}'
        }

    try:
        log.info(f"Manually triggering {connector_name} sync...")

        # Run the sync function
        asyncio.run(sync_functions[connector_name]())

        return {
            'success': True,
            'message': f'{connector_name} sync triggered successfully'
        }

    except Exception as e:
        log.error(f"Error triggering {connector_name} sync: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def get_scheduled_jobs() -> list:
    """
    Get list of all scheduled jobs

    Returns:
        List of job info dicts
    """
    jobs = []

    for job in scheduler.get_jobs():
        next_run = job.next_run_time

        jobs.append({
            'id': job.id,
            'name': job.name,
            'next_run': next_run.isoformat() if next_run else None,
            'trigger': str(job.trigger)
        })

    return jobs


def pause_job(job_id: str) -> bool:
    """
    Pause a scheduled job

    Args:
        job_id: Job ID to pause

    Returns:
        True if successful
    """
    try:
        scheduler.pause_job(job_id)
        log.info(f"Paused job: {job_id}")
        return True

    except Exception as e:
        log.error(f"Error pausing job {job_id}: {str(e)}")
        return False


def resume_job(job_id: str) -> bool:
    """
    Resume a paused job

    Args:
        job_id: Job ID to resume

    Returns:
        True if successful
    """
    try:
        scheduler.resume_job(job_id)
        log.info(f"Resumed job: {job_id}")
        return True

    except Exception as e:
        log.error(f"Error resuming job {job_id}: {str(e)}")
        return False


# CLI for manual syncs

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m app.scheduler <command> [connector_name]")
        print("\nCommands:")
        print("  start              Start the scheduler")
        print("  sync <connector>   Manually run a sync")
        print("  list               List all scheduled jobs")
        print("\nConnectors:")
        print("  google_ads, ga4, search_console, klaviyo, hotjar, github")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":
        print("Starting scheduler...")
        start_scheduler()

        # Keep running
        try:
            while True:
                asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("\nShutting down scheduler...")
            stop_scheduler()

    elif command == "sync":
        if len(sys.argv) < 3:
            print("Error: Please specify a connector name")
            print("Usage: python -m app.scheduler sync <connector_name>")
            sys.exit(1)

        connector_name = sys.argv[2]
        result = run_sync_now(connector_name)

        if result['success']:
            print(f"✓ {result['message']}")
        else:
            print(f"✗ Error: {result['error']}")
            sys.exit(1)

    elif command == "list":
        print("\nScheduled Jobs:")
        print("-" * 80)

        jobs = get_scheduled_jobs()

        if not jobs:
            print("No jobs scheduled")
        else:
            for job in jobs:
                print(f"\nID:       {job['id']}")
                print(f"Name:     {job['name']}")
                print(f"Next Run: {job['next_run']}")
                print(f"Trigger:  {job['trigger']}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
