#!/usr/bin/env python3
"""
Chunked Historical Data Sync

Syncs data in manageable chunks to avoid memory/timeout issues.
Runs Shopify in yearly chunks, then other connectors.
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytz

SYDNEY_TZ = pytz.timezone('Australia/Sydney')

def print_header(text):
    print(f"\n{'='*70}")
    print(f"  {text}")
    print('='*70)

def print_status(msg):
    timestamp = datetime.now(SYDNEY_TZ).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")

async def sync_shopify_year(year: int):
    """Sync a single year of Shopify orders"""
    from app.connectors.shopify_connector import ShopifyConnector

    shopify = ShopifyConnector()

    # Calculate date range for this year
    now = datetime.now(SYDNEY_TZ)

    if year == now.year:
        # Current year - go from Jan 1 to now
        start_date = datetime(year, 1, 1, 0, 0, 0, tzinfo=SYDNEY_TZ)
        end_date = now.replace(hour=23, minute=59, second=59)
    else:
        # Past year - full year
        start_date = datetime(year, 1, 1, 0, 0, 0, tzinfo=SYDNEY_TZ)
        end_date = datetime(year, 12, 31, 23, 59, 59, tzinfo=SYDNEY_TZ)

    print_status(f"📦 Syncing Shopify {year}: {start_date.date()} to {end_date.date()}")

    result = await shopify.sync(start_date, end_date, include_products=False)

    if result['success']:
        orders_data = result['data']['orders']
        total_orders = orders_data.get('total_orders', 0)
        total_revenue = orders_data.get('total_revenue', 0)
        print_status(f"✅ {year}: {total_orders:,} orders, ${total_revenue:,.2f} revenue")
        return {
            'year': year,
            'orders': total_orders,
            'revenue': total_revenue,
            'success': True
        }
    else:
        print_status(f"❌ {year}: Failed - {result.get('error')}")
        return {'year': year, 'success': False, 'error': result.get('error')}

async def sync_other_connectors():
    """Sync all non-Shopify connectors"""
    from app.connectors.ga4_connector import GA4Connector
    from app.connectors.klaviyo_connector import KlaviyoConnector
    from app.connectors.search_console_connector import SearchConsoleConnector
    from app.connectors.merchant_center_connector import MerchantCenterConnector
    from app.connectors.github_connector import GitHubConnector

    now = datetime.now(SYDNEY_TZ)
    results = {}

    # GA4 - 14 months
    print_header("GA4 (14 months)")
    try:
        ga4 = GA4Connector()
        start = (now - timedelta(days=425)).replace(hour=0, minute=0, second=0)
        end = now.replace(hour=23, minute=59, second=59)
        result = await ga4.sync(start, end)
        if result['success']:
            sessions = result['data'].get('totals', {}).get('sessions', 0)
            print_status(f"✅ GA4: {sessions:,} sessions")
            results['ga4'] = {'success': True, 'sessions': sessions}
        else:
            print_status(f"❌ GA4: {result.get('error')}")
            results['ga4'] = {'success': False}
    except Exception as e:
        print_status(f"❌ GA4 Error: {str(e)}")
        results['ga4'] = {'success': False, 'error': str(e)}

    # Search Console - 16 months
    print_header("Search Console (16 months)")
    try:
        sc = SearchConsoleConnector()
        start = (now - timedelta(days=480)).replace(hour=0, minute=0, second=0)
        end = now.replace(hour=23, minute=59, second=59)
        result = await sc.sync(start, end)
        if result['success']:
            queries = result['data'].get('query_performance', {}).get('total_queries', 0)
            print_status(f"✅ Search Console: {queries:,} queries")
            results['search_console'] = {'success': True, 'queries': queries}
        else:
            print_status(f"❌ Search Console: {result.get('error')}")
            results['search_console'] = {'success': False}
    except Exception as e:
        print_status(f"❌ Search Console Error: {str(e)}")
        results['search_console'] = {'success': False, 'error': str(e)}

    # Klaviyo - 3 years
    print_header("Klaviyo (3 years)")
    try:
        klaviyo = KlaviyoConnector()
        start = (now - timedelta(days=1095)).replace(hour=0, minute=0, second=0)
        end = now.replace(hour=23, minute=59, second=59)
        result = await klaviyo.sync(start, end)
        if result['success']:
            flows = result['data'].get('flows_count', 0)
            lists = result['data'].get('lists_count', 0)
            print_status(f"✅ Klaviyo: {flows} flows, {lists} lists")
            results['klaviyo'] = {'success': True, 'flows': flows, 'lists': lists}
        else:
            print_status(f"❌ Klaviyo: {result.get('error')}")
            results['klaviyo'] = {'success': False}
    except Exception as e:
        print_status(f"❌ Klaviyo Error: {str(e)}")
        results['klaviyo'] = {'success': False, 'error': str(e)}

    # Merchant Center - current
    print_header("Merchant Center (current)")
    try:
        mc = MerchantCenterConnector()
        result = await mc.sync(now, now)
        if result['success']:
            products = result['data'].get('product_status_summary', {})
            approved = products.get('approved', 0)
            disapproved = products.get('disapproved', 0)
            print_status(f"✅ Merchant Center: {approved:,} approved, {disapproved:,} disapproved")
            results['merchant_center'] = {'success': True, 'approved': approved, 'disapproved': disapproved}
        else:
            print_status(f"❌ Merchant Center: {result.get('error')}")
            results['merchant_center'] = {'success': False}
    except Exception as e:
        print_status(f"❌ Merchant Center Error: {str(e)}")
        results['merchant_center'] = {'success': False, 'error': str(e)}

    # GitHub - 3 years
    print_header("GitHub (3 years)")
    try:
        github = GitHubConnector()
        start = (now - timedelta(days=1095)).replace(hour=0, minute=0, second=0)
        end = now.replace(hour=23, minute=59, second=59)
        result = await github.sync(start, end)
        if result['success']:
            commits = result['data'].get('commits_count', 0)
            print_status(f"✅ GitHub: {commits} commits")
            results['github'] = {'success': True, 'commits': commits}
        else:
            print_status(f"❌ GitHub: {result.get('error')}")
            results['github'] = {'success': False}
    except Exception as e:
        print_status(f"❌ GitHub Error: {str(e)}")
        results['github'] = {'success': False, 'error': str(e)}

    return results

async def main():
    print("\n" + "="*70)
    print("  ML-Audit Chunked Historical Data Sync")
    print("="*70)

    start_time = datetime.now()

    # Sync Shopify year by year (3 years: 2023, 2024, 2025, 2026)
    print_header("SHOPIFY (3 YEARS - Chunked by Year)")

    current_year = datetime.now(SYDNEY_TZ).year
    years_to_sync = [current_year - 2, current_year - 1, current_year]  # Last 3 years

    shopify_results = []
    total_orders = 0
    total_revenue = 0.0

    for year in years_to_sync:
        result = await sync_shopify_year(year)
        shopify_results.append(result)
        if result.get('success'):
            total_orders += result.get('orders', 0)
            total_revenue += result.get('revenue', 0)

    print_status(f"\n📊 Shopify Total: {total_orders:,} orders, ${total_revenue:,.2f}")

    # Sync other connectors
    other_results = await sync_other_connectors()

    # Summary
    duration = (datetime.now() - start_time).total_seconds()

    print("\n" + "="*70)
    print("  SYNC COMPLETE")
    print("="*70)
    print(f"\n📊 Total Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print("\n📦 Shopify Summary:")
    print(f"   Total Orders: {total_orders:,}")
    print(f"   Total Revenue: ${total_revenue:,.2f}")

    print("\n📈 Other Connectors:")
    for name, result in other_results.items():
        status = "✅" if result.get('success') else "❌"
        print(f"   {status} {name.replace('_', ' ').title()}")

if __name__ == "__main__":
    asyncio.run(main())
