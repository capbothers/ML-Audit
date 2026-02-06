"""
Scheduler for automated data connector syncs

Uses APScheduler to run connector syncs at optimal intervals.
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio
from typing import Optional

from app.connectors.google_ads import GoogleAdsConnector
from app.connectors.klaviyo import KlaviyoConnector
from app.connectors.hotjar import HotjarConnector
from app.connectors.github import GitHubConnector
from app.services.data_sync_service import DataSyncService, SyncResult, update_data_sync_status
from app.services.ml_intelligence_service import MLIntelligenceService
from app.services.caprice_import_service import CapriceImportService
from app.services.google_ads_sheet_import import GoogleAdsSheetImportService
from app.models.base import get_db
from app.config import get_settings
from app.utils.logger import log
from app.utils.cache import clear_cache
import time

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

settings = get_settings()
scheduler = AsyncIOScheduler()


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


async def sync_ga4():
    """Sync GA4 data (twice daily) using DataSyncService"""
    try:
        log.info("Starting GA4 sync...")
        sync_service = DataSyncService()
        # GA4 has 24-48h delay, sync last 5 days to cover weekends and late processing
        result = await sync_service.sync_ga4(days=5)

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


async def sync_caprice_pricing():
    """Import new Caprice pricing files (daily at 1pm)"""
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


async def sync_shopify_full():
    """Full Shopify sync including products (daily at 1am AEST)"""
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
            clear_cache()
        else:
            log.error(f"Shopify full sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Shopify full sync error: {str(e)}")


async def sync_google_ads_sheet():
    """Import Google Ads data from Google Sheet (daily at 6am AEST)"""
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
        clear_cache()

    except Exception as e:
        log.error(f"Google Ads Sheet import error: {str(e)}")


async def sync_cost_sheet():
    """Sync NETT Master cost sheet from Google Sheets (daily at 4:30am AEST)"""
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
            clear_cache()
        else:
            log.error(f"Cost sheet sync failed: {result.get('error')}")

    except Exception as e:
        log.error(f"Cost sheet sync error: {str(e)}")


# Schedule Configuration

def setup_scheduler():
    """
    Configure and start the scheduler.

    All cron times are Australia/Sydney (AEST/AEDT).

    Sync Frequencies:
    - Shopify orders:     Every 2 hours (orders + order_items for COGS/P&L)
    - Shopify full:       Daily 1:00am  (includes products/variants catalog)
    - Google Ads (Sheet): Daily 6:00am  (campaign + product data from Google Sheets)
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

    # ── Shopify ──────────────────────────────────────────
    # Orders only - every 2 hours
    scheduler.add_job(
        sync_shopify,
        trigger=IntervalTrigger(hours=2),
        id='shopify_sync',
        name='Shopify Orders & Items Sync',
        replace_existing=True,
        max_instances=1
    )
    # Full sync (with products) - daily at 1am AEST
    scheduler.add_job(
        sync_shopify_full,
        trigger=CronTrigger(hour=1, minute=0, timezone=SYDNEY_TZ),
        id='shopify_full_sync',
        name='Shopify Full Sync (with products)',
        replace_existing=True,
        max_instances=1
    )

    # ── Google Ads ───────────────────────────────────────
    # Sheet import - daily at 6am AEST
    scheduler.add_job(
        sync_google_ads_sheet,
        trigger=CronTrigger(hour=6, minute=0, timezone=SYDNEY_TZ),
        id='google_ads_sheet_sync',
        name='Google Ads Sheet Daily Import',
        replace_existing=True,
        max_instances=1
    )

    # ── Cost Sheet (NETT Master) ─────────────────────────
    # Daily at 4:30am AEST
    scheduler.add_job(
        sync_cost_sheet,
        trigger=CronTrigger(hour=4, minute=30, timezone=SYDNEY_TZ),
        id='cost_sheet_sync',
        name='Cost Sheet Daily Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── GA4 ──────────────────────────────────────────────
    # Twice daily: 4am and 4pm AEST (5-day window covers processing delay)
    scheduler.add_job(
        sync_ga4,
        trigger=CronTrigger(hour=4, minute=0, timezone=SYDNEY_TZ),
        id='ga4_sync_morning',
        name='GA4 Morning Sync',
        replace_existing=True,
        max_instances=1
    )
    scheduler.add_job(
        sync_ga4,
        trigger=CronTrigger(hour=16, minute=0, timezone=SYDNEY_TZ),
        id='ga4_sync_afternoon',
        name='GA4 Afternoon Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── Search Console ───────────────────────────────────
    # Daily at 5am AEST
    scheduler.add_job(
        sync_search_console,
        trigger=CronTrigger(hour=5, minute=0, timezone=SYDNEY_TZ),
        id='search_console_sync',
        name='Search Console Daily Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── Merchant Center ──────────────────────────────────
    # Daily at 2am AEST
    scheduler.add_job(
        sync_merchant_center,
        trigger=CronTrigger(hour=2, minute=0, timezone=SYDNEY_TZ),
        id='merchant_center_sync',
        name='Merchant Center Daily Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── Klaviyo ──────────────────────────────────────────
    # Every hour
    scheduler.add_job(
        sync_klaviyo,
        trigger=IntervalTrigger(hours=1),
        id='klaviyo_sync',
        name='Klaviyo Hourly Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── Hotjar/Clarity ───────────────────────────────────
    # Daily at 6:30am AEST
    scheduler.add_job(
        sync_hotjar,
        trigger=CronTrigger(hour=6, minute=30, timezone=SYDNEY_TZ),
        id='hotjar_sync',
        name='Hotjar/Clarity Daily Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── GitHub ───────────────────────────────────────────
    # Daily at 7am AEST
    scheduler.add_job(
        sync_github,
        trigger=CronTrigger(hour=7, minute=0, timezone=SYDNEY_TZ),
        id='github_sync',
        name='GitHub Daily Sync',
        replace_existing=True,
        max_instances=1
    )

    # ── ML Intelligence ──────────────────────────────────
    # Daily at 3am AEST (after overnight data syncs complete)
    scheduler.add_job(
        run_ml_intelligence,
        trigger=CronTrigger(hour=3, minute=0, timezone=SYDNEY_TZ),
        id='ml_intelligence',
        name='ML Intelligence Daily Pipeline',
        replace_existing=True,
        max_instances=1
    )

    # ── Caprice Pricing ──────────────────────────────────
    # Daily at 1pm AEST
    scheduler.add_job(
        sync_caprice_pricing,
        trigger=CronTrigger(hour=13, minute=0, timezone=SYDNEY_TZ),
        id='caprice_pricing_import',
        name='Caprice Pricing Daily Import',
        replace_existing=True,
        max_instances=1
    )

    log.info("Scheduler configured with all sync jobs (timezone: Australia/Sydney)")


def start_scheduler():
    """Start the scheduler"""
    setup_scheduler()
    scheduler.start()
    log.info("Scheduler started")


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
