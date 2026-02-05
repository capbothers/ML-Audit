#!/usr/bin/env python3
"""
Full Historical Data Sync Script - MAXIMUM HISTORY

Syncs all data sources with maximum available history:
- Shopify: 1095 days (3 years) - orders, products, customers
- GA4: 425 days (14 months max API limit)
- Search Console: 480 days (16 months max)
- Klaviyo: 1095 days (3 years) - campaigns, flows, segments
- Google Sheets: current data
- GitHub: all commits and current files
- Merchant Center: current feed status
"""
import asyncio
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import pytz

# Add parent directory to path
sys.path.insert(0, '/workspaces/ML-Audit')

from app.connectors.shopify_connector import ShopifyConnector
from app.connectors.ga4_connector import GA4Connector
from app.connectors.klaviyo_connector import KlaviyoConnector
from app.connectors.merchant_center_connector import MerchantCenterConnector
from app.connectors.github_connector import GitHubConnector
from app.connectors.search_console_connector import SearchConsoleConnector
from app.config import get_settings

# Sydney timezone
SYDNEY_TZ = pytz.timezone('Australia/Sydney')

# ANSI colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


def log(message: str, color: str = ''):
    """Log with timestamp and optional color"""
    timestamp = datetime.now(SYDNEY_TZ).strftime('%Y-%m-%d %H:%M:%S')
    print(f"{color}[{timestamp}] {message}{Colors.END}")


def log_header(title: str):
    """Log section header"""
    print()
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}{Colors.END}")
    print()


def log_success(message: str):
    log(f"✅ {message}", Colors.GREEN)


def log_error(message: str):
    log(f"❌ {message}", Colors.RED)


def log_info(message: str):
    log(f"📊 {message}", Colors.CYAN)


def log_progress(message: str):
    log(f"⏳ {message}", Colors.YELLOW)


def get_date_range(days: int) -> tuple:
    """Get date range in Sydney timezone"""
    now = datetime.now(SYDNEY_TZ)
    end_date = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_date = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start_date, end_date


def extract_shopify_stats(data: Dict) -> Dict:
    """Extract stats from Shopify sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    return {
        'orders': len(result.get('orders', [])),
        'products': len(result.get('products', [])),
        'customers': len(result.get('customers', [])),
        'abandoned_checkouts': len(result.get('abandoned_checkouts', [])),
        'total_revenue': sum(float(o.get('total_price', 0)) for o in result.get('orders', []))
    }


def extract_ga4_stats(data: Dict) -> Dict:
    """Extract stats from GA4 sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    traffic = result.get('traffic_overview', {})
    daily_metrics = traffic.get('daily_metrics', [])

    return {
        'days': len(daily_metrics),
        'total_sessions': sum(d.get('sessions', 0) for d in daily_metrics),
        'total_users': sum(d.get('active_users', 0) for d in daily_metrics),
        'total_pageviews': sum(d.get('pageviews', 0) for d in daily_metrics),
        'pages_tracked': len(result.get('pages', [])),
        'traffic_sources': len(result.get('traffic_sources', []))
    }


def extract_search_console_stats(data: Dict) -> Dict:
    """Extract stats from Search Console sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    query_perf = result.get('query_performance', {})
    page_perf = result.get('page_performance', {})

    return {
        'queries': query_perf.get('total_queries', 0),
        'pages': page_perf.get('total_pages', 0),
        'total_clicks': query_perf.get('total_clicks', 0),
        'total_impressions': query_perf.get('total_impressions', 0),
        'avg_ctr': query_perf.get('avg_ctr', 0),
        'devices': len(result.get('device_breakdown', [])),
        'countries': len(result.get('country_breakdown', [])),
    }


def extract_klaviyo_stats(data: Dict) -> Dict:
    """Extract stats from Klaviyo sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    return {
        'flows': len(result.get('flows', [])),
        'campaigns': len(result.get('campaigns', [])),
        'lists': len(result.get('lists', [])),
        'segments': len(result.get('segments', [])),
        'metrics': len(result.get('metrics', []))
    }


def extract_merchant_center_stats(data: Dict) -> Dict:
    """Extract stats from Merchant Center sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    statuses = result.get('product_statuses', {})

    return {
        'approved': statuses.get('approved', 0),
        'disapproved': statuses.get('disapproved', 0),
        'pending': statuses.get('pending', 0),
        'issues': len(statuses.get('issues', []))
    }


def extract_github_stats(data: Dict) -> Dict:
    """Extract stats from GitHub sync result"""
    if not data.get('success') or 'data' not in data:
        return {}

    result = data['data']
    repo = result.get('repository', {})
    prs = result.get('pull_requests', {})

    return {
        'repo_name': repo.get('full_name', 'Unknown'),
        'commits': len(result.get('recent_commits', [])),
        'branches': len(result.get('branches', [])),
        'open_prs': prs.get('open_count', 0),
        'critical_files': len(result.get('critical_files', []))
    }


async def sync_shopify(days: int = 1095) -> Dict:
    """Sync Shopify data - 3 years history"""
    log_progress(f"Syncing Shopify ({days} days = {days//365} years of history)...")
    start_time = time.time()

    try:
        connector = ShopifyConnector()
        start_date, end_date = get_date_range(days)

        log_info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        result = await connector.sync(start_date, end_date, include_products=True)

        elapsed = time.time() - start_time
        stats = extract_shopify_stats(result)

        if result.get('success'):
            log_success(f"Shopify sync completed in {elapsed:.1f}s ({elapsed/60:.1f} min)")
            log_info(f"  Orders: {stats.get('orders', 0):,}")
            log_info(f"  Products: {stats.get('products', 0):,}")
            log_info(f"  Customers: {stats.get('customers', 0):,}")
            log_info(f"  Abandoned Checkouts: {stats.get('abandoned_checkouts', 0):,}")
            log_info(f"  Total Revenue: ${stats.get('total_revenue', 0):,.2f}")
        else:
            log_error(f"Shopify sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"Shopify sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def sync_ga4(days: int = 425) -> Dict:
    """Sync GA4 data - 14 months (max API limit)"""
    log_progress(f"Syncing GA4 ({days} days = ~{days//30} months of history)...")
    start_time = time.time()

    try:
        connector = GA4Connector()
        start_date, end_date = get_date_range(days)

        log_info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        result = await connector.sync(start_date, end_date)

        elapsed = time.time() - start_time
        stats = extract_ga4_stats(result)

        if result.get('success'):
            log_success(f"GA4 sync completed in {elapsed:.1f}s")
            log_info(f"  Days of data: {stats.get('days', 0)}")
            log_info(f"  Total Sessions: {stats.get('total_sessions', 0):,}")
            log_info(f"  Total Users: {stats.get('total_users', 0):,}")
            log_info(f"  Total Pageviews: {stats.get('total_pageviews', 0):,}")
            log_info(f"  Pages Tracked: {stats.get('pages_tracked', 0)}")
            log_info(f"  Traffic Sources: {stats.get('traffic_sources', 0)}")
        else:
            log_error(f"GA4 sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"GA4 sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def sync_search_console(days: int = 480) -> Dict:
    """Sync Search Console data - 16 months (max API limit)"""
    log_progress(f"Syncing Search Console ({days} days = ~{days//30} months of history)...")
    start_time = time.time()

    try:
        connector = SearchConsoleConnector()
        start_date, end_date = get_date_range(days)

        log_info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        result = await connector.sync(start_date, end_date)

        elapsed = time.time() - start_time
        stats = extract_search_console_stats(result)

        if result.get('success'):
            log_success(f"Search Console sync completed in {elapsed:.1f}s")
            log_info(f"  Total Queries: {stats.get('queries', 0):,}")
            log_info(f"  Total Pages: {stats.get('pages', 0):,}")
            log_info(f"  Total Clicks: {stats.get('total_clicks', 0):,}")
            log_info(f"  Total Impressions: {stats.get('total_impressions', 0):,}")
            log_info(f"  Average CTR: {stats.get('avg_ctr', 0) * 100:.2f}%")
            log_info(f"  Devices Tracked: {stats.get('devices', 0)}")
            log_info(f"  Countries Tracked: {stats.get('countries', 0)}")
        else:
            log_error(f"Search Console sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"Search Console sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def sync_klaviyo(days: int = 1095) -> Dict:
    """Sync Klaviyo data - 3 years history"""
    log_progress(f"Syncing Klaviyo ({days} days = {days//365} years of history)...")
    start_time = time.time()

    try:
        connector = KlaviyoConnector()
        start_date, end_date = get_date_range(days)

        log_info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        result = await connector.sync(start_date, end_date)

        elapsed = time.time() - start_time
        stats = extract_klaviyo_stats(result)

        if result.get('success'):
            log_success(f"Klaviyo sync completed in {elapsed:.1f}s")
            log_info(f"  Flows: {stats.get('flows', 0)}")
            log_info(f"  Campaigns: {stats.get('campaigns', 0)}")
            log_info(f"  Lists: {stats.get('lists', 0)}")
            log_info(f"  Segments: {stats.get('segments', 0)}")
            log_info(f"  Metrics: {stats.get('metrics', 0)}")
        else:
            log_error(f"Klaviyo sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"Klaviyo sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def sync_merchant_center() -> Dict:
    """Sync Merchant Center data - current feed status"""
    log_progress("Syncing Google Merchant Center (current feed status)...")
    start_time = time.time()

    try:
        connector = MerchantCenterConnector()
        start_date, end_date = get_date_range(0)

        result = await connector.sync(start_date, end_date)

        elapsed = time.time() - start_time
        stats = extract_merchant_center_stats(result)

        if result.get('success'):
            log_success(f"Merchant Center sync completed in {elapsed:.1f}s")
            log_info(f"  Approved Products: {stats.get('approved', 0):,}")
            log_info(f"  Disapproved Products: {stats.get('disapproved', 0):,}")
            log_info(f"  Pending Products: {stats.get('pending', 0):,}")
            log_info(f"  Issue Types: {stats.get('issues', 0)}")
        else:
            log_error(f"Merchant Center sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"Merchant Center sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def sync_github(days: int = 1095) -> Dict:
    """Sync GitHub data - 3 years history"""
    log_progress(f"Syncing GitHub ({days} days = {days//365} years of commits)...")
    start_time = time.time()

    try:
        connector = GitHubConnector()
        start_date, end_date = get_date_range(days)

        log_info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        result = await connector.sync(start_date, end_date)

        elapsed = time.time() - start_time
        stats = extract_github_stats(result)

        if result.get('success'):
            log_success(f"GitHub sync completed in {elapsed:.1f}s")
            log_info(f"  Repository: {stats.get('repo_name', 'Unknown')}")
            log_info(f"  Recent Commits: {stats.get('commits', 0)}")
            log_info(f"  Branches: {stats.get('branches', 0)}")
            log_info(f"  Open PRs: {stats.get('open_prs', 0)}")
            log_info(f"  Critical Files Tracked: {stats.get('critical_files', 0)}")
        else:
            log_error(f"GitHub sync failed: {result.get('error', 'Unknown error')}")

        return {'success': result.get('success'), 'stats': stats, 'duration': elapsed}

    except Exception as e:
        elapsed = time.time() - start_time
        log_error(f"GitHub sync error: {str(e)}")
        return {'success': False, 'error': str(e), 'duration': elapsed}


async def main():
    """Run full historical sync with maximum history"""
    log_header("ML-Audit Full Historical Data Sync - MAXIMUM HISTORY")

    total_start = time.time()
    settings = get_settings()

    log_info(f"Starting full sync at {datetime.now(SYDNEY_TZ).strftime('%Y-%m-%d %H:%M:%S AEST')}")
    print()
    print(f"{Colors.BOLD}Sync Configuration:{Colors.END}")
    print(f"  • Shopify: 1095 days (3 years)")
    print(f"  • GA4: 425 days (14 months - API max)")
    print(f"  • Search Console: 480 days (16 months - API max)")
    print(f"  • Klaviyo: 1095 days (3 years)")
    print(f"  • GitHub: 1095 days (3 years)")
    print(f"  • Merchant Center: current status")
    print()

    results = {}

    # 1. Shopify (3 years)
    log_header("1. SHOPIFY (3 YEARS)")
    results['shopify'] = await sync_shopify(1095)
    print()

    # 2. GA4 (14 months - API limit)
    log_header("2. GOOGLE ANALYTICS 4 (14 MONTHS)")
    results['ga4'] = await sync_ga4(425)
    print()

    # 3. Search Console (16 months - API limit)
    log_header("3. GOOGLE SEARCH CONSOLE (16 MONTHS)")
    results['search_console'] = await sync_search_console(480)
    print()

    # 4. Klaviyo (3 years)
    log_header("4. KLAVIYO (3 YEARS)")
    results['klaviyo'] = await sync_klaviyo(1095)
    print()

    # 5. Merchant Center (current)
    log_header("5. GOOGLE MERCHANT CENTER")
    results['merchant_center'] = await sync_merchant_center()
    print()

    # 6. GitHub (3 years)
    log_header("6. GITHUB (3 YEARS)")
    results['github'] = await sync_github(1095)
    print()

    # Summary
    total_elapsed = time.time() - total_start

    log_header("SYNC SUMMARY")

    success_count = sum(1 for r in results.values() if r.get('success'))
    total_count = len(results)

    print(f"{Colors.BOLD}Overall: {success_count}/{total_count} connectors synced successfully{Colors.END}")
    print(f"{Colors.BOLD}Total Time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes){Colors.END}")
    print()

    # Detailed summary table
    print(f"{Colors.BOLD}{'Connector':<20} {'Status':<10} {'Duration':<12} {'Key Stats'}{Colors.END}")
    print("-" * 90)

    # Shopify
    shopify = results.get('shopify', {})
    status = "✅ OK" if shopify.get('success') else "❌ FAIL"
    duration = f"{shopify.get('duration', 0):.1f}s"
    stats = shopify.get('stats', {})
    stats_str = f"{stats.get('orders', 0):,} orders, {stats.get('products', 0):,} products, ${stats.get('total_revenue', 0):,.0f}"
    print(f"{'Shopify':<20} {status:<10} {duration:<12} {stats_str}")

    # GA4
    ga4 = results.get('ga4', {})
    status = "✅ OK" if ga4.get('success') else "❌ FAIL"
    duration = f"{ga4.get('duration', 0):.1f}s"
    stats = ga4.get('stats', {})
    stats_str = f"{stats.get('days', 0)} days, {stats.get('total_sessions', 0):,} sessions, {stats.get('total_pageviews', 0):,} pageviews"
    print(f"{'GA4':<20} {status:<10} {duration:<12} {stats_str}")

    # Search Console
    gsc = results.get('search_console', {})
    status = "✅ OK" if gsc.get('success') else "❌ FAIL"
    duration = f"{gsc.get('duration', 0):.1f}s"
    stats = gsc.get('stats', {})
    stats_str = f"{stats.get('queries', 0):,} queries, {stats.get('pages', 0):,} pages, {stats.get('total_clicks', 0):,} clicks"
    print(f"{'Search Console':<20} {status:<10} {duration:<12} {stats_str}")

    # Klaviyo
    klaviyo = results.get('klaviyo', {})
    status = "✅ OK" if klaviyo.get('success') else "❌ FAIL"
    duration = f"{klaviyo.get('duration', 0):.1f}s"
    stats = klaviyo.get('stats', {})
    stats_str = f"{stats.get('flows', 0)} flows, {stats.get('campaigns', 0)} campaigns, {stats.get('lists', 0)} lists"
    print(f"{'Klaviyo':<20} {status:<10} {duration:<12} {stats_str}")

    # Merchant Center
    mc = results.get('merchant_center', {})
    status = "✅ OK" if mc.get('success') else "❌ FAIL"
    duration = f"{mc.get('duration', 0):.1f}s"
    stats = mc.get('stats', {})
    stats_str = f"{stats.get('approved', 0):,} approved, {stats.get('disapproved', 0):,} disapproved"
    print(f"{'Merchant Center':<20} {status:<10} {duration:<12} {stats_str}")

    # GitHub
    github = results.get('github', {})
    status = "✅ OK" if github.get('success') else "❌ FAIL"
    duration = f"{github.get('duration', 0):.1f}s"
    stats = github.get('stats', {})
    stats_str = f"{stats.get('commits', 0)} commits, {stats.get('branches', 0)} branches, {stats.get('open_prs', 0)} open PRs"
    print(f"{'GitHub':<20} {status:<10} {duration:<12} {stats_str}")

    print("-" * 90)
    print()

    log_success(f"Full sync completed at {datetime.now(SYDNEY_TZ).strftime('%Y-%m-%d %H:%M:%S AEST')}")

    return results


if __name__ == "__main__":
    asyncio.run(main())
