# Data Backfill Instructions

This document provides step-by-step instructions for backfilling historical data after the data persistence improvements.

## Prerequisites

1. Ensure your `.env` file has all required API credentials configured
2. Database connection is working
3. Virtual environment is activated

## Step 1: Run Database Migrations

First, apply the new database migrations to create the required tables:

```bash
# From the project root directory
cd /workspaces/ML-Audit

# Run migrations
alembic upgrade head
```

This will create:
- `merchant_center_product_statuses` - Daily product status snapshots
- `merchant_center_disapprovals` - Historical disapproval tracking
- `merchant_center_account_statuses` - Account-level metrics over time
- `ga4_daily_ecommerce` - Daily e-commerce totals (ecommercePurchases for Shopify reconciliation)
- Adds `total_revenue` column to `ga4_events` for conversion event revenue tracking

## Step 2: Verify Tables Exist

You can verify the tables were created:

```bash
# Using SQLite (if using SQLite)
sqlite3 data/ml_audit.db ".tables"

# Or check via Python
python -c "from app.models.base import engine; print(engine.table_names())"
```

## Step 3: Backfill Shopify Data

The Shopify sync now saves products, customers, and refunds (not just orders).

### Option A: Full Historical Sync (Recommended)

```python
# Run from Python or create a script
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_shopify():
    sync = DataSyncService()

    # Sync last 365 days of data
    result = await sync.sync_shopify(days=365, include_products=True, save_to_db=True)

    print(f"Orders saved: {result.get('orders_saved', 0)}")
    print(f"Products saved: {result.get('products_saved', 0)}")
    print(f"Customers saved: {result.get('customers_saved', 0)}")
    print(f"Refunds saved: {result.get('refunds_saved', 0)}")

asyncio.run(backfill_shopify())
```

### Option B: Via API Endpoint

```bash
# Trigger sync via API (if server is running)
curl -X POST "http://localhost:8000/sync/shopify?days=365"
```

## Step 4: Backfill Google Ads Data

Google Ads data is now persisted to `google_ads_campaigns`, `google_ads_ad_groups`, and `google_ads_search_terms`.

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_google_ads():
    sync = DataSyncService()

    # Sync last 90 days of Google Ads data
    result = await sync.sync_google_ads(days=90, save_to_db=True)

    print(f"Campaigns saved: {result.get('campaigns_saved', 0)}")
    print(f"Ad groups saved: {result.get('ad_groups_saved', 0)}")
    print(f"Search terms saved: {result.get('search_terms_saved', 0)}")

asyncio.run(backfill_google_ads())
```

**Note:** Google Ads API returns aggregated data. For daily granularity, run syncs daily.

## Step 5: Backfill Klaviyo Flow Messages

Klaviyo flow messages with metrics are now tracked in `klaviyo_flow_messages`.

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_klaviyo():
    sync = DataSyncService()

    # Sync Klaviyo data (flows, campaigns, segments, flow messages)
    result = await sync.sync_klaviyo(days=90, save_to_db=True)

    print(f"Campaigns saved: {result.get('saved_to_db', 0)}")
    print(f"Updated: {result.get('updated_in_db', 0)}")

asyncio.run(backfill_klaviyo())
```

## Step 6: Backfill Merchant Center Data

Merchant Center disapproval history is now tracked. Run daily to build history.

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_merchant_center():
    sync = DataSyncService()

    # Sync Merchant Center (creates today's snapshot)
    result = await sync.sync_merchant_center(quick=False, save_to_db=True)

    print(f"Product statuses saved: {result.get('statuses_saved', 0)}")
    print(f"Disapprovals saved: {result.get('disapprovals_saved', 0)}")
    print(f"Account status saved: {result.get('account_status_saved', False)}")

asyncio.run(backfill_merchant_center())
```

**Note:** Merchant Center only provides current state, not historical. Run daily to build history over time.

## Step 7: Backfill Search Console Data

Search Console supports up to 16 months of historical data. Use the backfill endpoint for large historical syncs.

### Option A: Via API Endpoint (Recommended)

```bash
# Full 16-month backfill with default settings
curl -X POST "http://localhost:8000/sync/search-console/backfill?months=16"

# Custom backfill with adjusted settings
curl -X POST "http://localhost:8000/sync/search-console/backfill?months=12&window_days=14&delay=2.0"

# Parameters:
# - months: 1-16 (default 16)
# - window_days: 7-30 days per fetch window (default 14)
# - delay: seconds between windows for rate limiting (default 2.0)
```

### Option B: Via Python Script

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_search_console():
    sync = DataSyncService()

    # Backfill last 16 months
    result = await sync.backfill_search_console(
        months=16,
        window_days=14,  # 14-day windows
        delay_between_windows=2.0  # 2 second delay between windows
    )

    print(f"Success: {result.get('success')}")
    print(f"Windows processed: {result.get('windows_processed')}")
    print(f"Windows failed: {result.get('windows_failed')}")
    print(f"Total queries: {result.get('total_queries')}")
    print(f"Total pages: {result.get('total_pages')}")
    print(f"Duration: {result.get('duration_seconds')}s")

    # Check for any errors
    if result.get('errors'):
        print("Errors:")
        for error in result['errors']:
            print(f"  Window {error['window']}: {error['error']}")

asyncio.run(backfill_search_console())
```

**Notes:**
- Search Console has a 2-3 day data delay; the most recent data available is from 3 days ago
- Backfill runs in chunks (windows) to avoid rate limits
- Failed windows don't stop the entire backfill; errors are logged per-window
- Use smaller window_days (7) if hitting rate limits, larger (30) for faster sync
- Check `DataSyncLog` table for sync history and errors

### Daily Search Console Sync

After backfill, set up a daily sync for recent data:

```bash
# Daily sync via API (syncs last 3 days by default)
curl -X POST "http://localhost:8000/sync/search-console/daily"

# Custom days
curl -X POST "http://localhost:8000/sync/search-console/daily?days=5"
```

Or via Python:

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def daily_search_console():
    sync = DataSyncService()
    result = await sync.daily_sync_search_console(days=3)
    print(f"Success: {result.get('success')}")
    print(f"Queries synced: {result.get('queries')}")
    print(f"Pages synced: {result.get('pages')}")

asyncio.run(daily_search_console())
```

## Step 8: Full Sync (All Sources)

To backfill all sources at once:

```python
import asyncio
from app.services.data_sync_service import DataSyncService

async def backfill_all():
    sync = DataSyncService()

    # Full sync for last 90 days
    result = await sync.sync_all(days=90)

    print(f"Sync complete: {result.get('sources_synced')}/{result.get('total_sources')} sources")
    print(f"Total duration: {result.get('total_duration'):.2f}s")

    for source, data in result.get('results', {}).items():
        print(f"  {source}: {data.get('saved_to_db', 0)} saved, {data.get('updated_in_db', 0)} updated")

asyncio.run(backfill_all())
```

Or via API:

```bash
curl -X POST "http://localhost:8000/sync/all?days=90"
```

## Step 8: Schedule Daily Syncs

For ongoing data collection, set up a cron job or scheduled task:

### Using Cron (Linux/Mac)

```bash
# Edit crontab
crontab -e

# Add daily sync at 2 AM Sydney time
0 2 * * * cd /workspaces/ML-Audit && /usr/bin/python -c "import asyncio; from app.services.data_sync_service import DataSyncService; asyncio.run(DataSyncService().sync_all(days=1))"

# Add Search Console daily sync at 4 AM (after data delay)
0 4 * * * cd /workspaces/ML-Audit && /usr/bin/python -c "import asyncio; from app.services.data_sync_service import DataSyncService; asyncio.run(DataSyncService().daily_sync_search_console(days=3))"
```

### Using Python APScheduler

Add to your application startup:

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.services.data_sync_service import DataSyncService

scheduler = AsyncIOScheduler()

@scheduler.scheduled_job('cron', hour=2, minute=0)  # 2 AM daily
async def daily_sync():
    sync = DataSyncService()
    await sync.sync_all(days=1)

@scheduler.scheduled_job('cron', hour=4, minute=0)  # 4 AM daily (after GSC data delay)
async def daily_search_console_sync():
    sync = DataSyncService()
    await sync.daily_sync_search_console(days=3)

scheduler.start()
```

## Verification Queries

After backfilling, verify data with these queries:

```python
from app.models.base import SessionLocal
from app.models.shopify import ShopifyProduct, ShopifyCustomer, ShopifyRefund
from app.models.google_ads_data import GoogleAdsCampaign
from app.models.klaviyo_data import KlaviyoFlowMessage
from app.models.merchant_center_data import MerchantCenterAccountStatus

db = SessionLocal()

print(f"Shopify Products: {db.query(ShopifyProduct).count()}")
print(f"Shopify Customers: {db.query(ShopifyCustomer).count()}")
print(f"Shopify Refunds: {db.query(ShopifyRefund).count()}")
print(f"Google Ads Campaigns: {db.query(GoogleAdsCampaign).count()}")
print(f"Klaviyo Flow Messages: {db.query(KlaviyoFlowMessage).count()}")
print(f"Merchant Center Account Snapshots: {db.query(MerchantCenterAccountStatus).count()}")

db.close()
```

## Troubleshooting

### API Rate Limits

If you hit rate limits during backfill:
- Reduce the `days` parameter
- Add delays between syncs
- Use the `quick=True` option for faster syncs

### Connection Errors

If connectors fail to connect:
1. Verify credentials in `.env`
2. Check API access/permissions
3. Review logs in `logs/ml_audit.log`

### Missing Data

If data isn't being saved:
1. Check sync logs: `DataSyncLog` table
2. Check validation failures: `ValidationFailure` table
3. Review connector status: `GET /status`

## Data Retention

Consider implementing data retention policies:

```python
from datetime import datetime, timedelta
from app.models.base import SessionLocal
from app.models.merchant_center_data import MerchantCenterDisapproval

db = SessionLocal()

# Delete disapproval records older than 1 year
cutoff = datetime.now().date() - timedelta(days=365)
deleted = db.query(MerchantCenterDisapproval).filter(
    MerchantCenterDisapproval.snapshot_date < cutoff
).delete()

db.commit()
print(f"Deleted {deleted} old disapproval records")
db.close()
```
