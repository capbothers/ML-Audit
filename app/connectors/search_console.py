"""
Google Search Console Connector (DEPRECATED)

DEPRECATED: Use search_console_connector.py with DataSyncService instead.
This connector was designed for direct database access pattern.
The new search_console_connector.py + DataSyncService pattern provides:
- Better separation of concerns (fetch vs persist)
- Centralized data validation
- Consistent sync logging
- Backfill and daily sync support

Syncs query performance, page rankings, index coverage, sitemaps, and rich results from Search Console API.
"""
import warnings
warnings.warn(
    "search_console.py is deprecated. Use search_console_connector.py with DataSyncService instead.",
    DeprecationWarning,
    stacklevel=2
)
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time
import asyncio

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    service_account = None
    build = None
    HttpError = Exception

from app.connectors.base import BaseConnector
from app.models.search_console_data import (
    SearchConsoleQuery, SearchConsolePage, SearchConsoleIndexCoverage,
    SearchConsoleSitemap, SearchConsoleRichResult
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class SearchConsoleConnector(BaseConnector):
    """
    Google Search Console API connector

    Syncs:
    - Query performance (clicks, impressions, CTR, position by query/page/device)
    - Page-level performance
    - Index coverage status
    - Sitemap submission status
    - Rich results (structured data) status
    """

    def __init__(self, db: Session):
        super().__init__(db, source_name="search_console", source_type="seo")
        self.service = None
        self.site_url = settings.gsc_site_url

    async def authenticate(self) -> bool:
        """
        Authenticate with Search Console API using service account

        Returns:
            True if authentication successful
        """
        try:
            if not service_account or not build:
                log.error("Google API libraries not installed. Install with: pip install google-api-python-client google-auth")
                return False

            if not settings.gsc_credentials_path or not settings.gsc_site_url:
                log.error("Missing Search Console credentials in settings")
                return False

            # Load credentials from JSON file
            credentials = service_account.Credentials.from_service_account_file(
                settings.gsc_credentials_path,
                scopes=['https://www.googleapis.com/auth/webmasters.readonly']
            )

            # Initialize service
            self.service = build('searchconsole', 'v1', credentials=credentials)

            # Test authentication with simple query
            site_list = self.service.sites().list().execute()

            # Verify our site URL is in the list
            if 'siteEntry' in site_list:
                sites = [site['siteUrl'] for site in site_list['siteEntry']]
                if self.site_url not in sites:
                    log.error(f"Site {self.site_url} not found in Search Console. Available sites: {sites}")
                    return False

            self._authenticated = True
            log.info("Search Console authentication successful")
            return True

        except Exception as e:
            log.error(f"Search Console authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync data from Search Console

        Args:
            start_date: Start date for sync (defaults to last sync or 16 months ago)
            end_date: End date for sync (defaults to 3 days ago due to data delay)

        Returns:
            Dict with sync results
        """
        sync_start_time = time.time()

        try:
            # Authenticate if needed
            if not self._authenticated:
                if not await self.authenticate():
                    raise Exception("Authentication failed")

            # Log sync start
            await self.log_sync_start()

            # Determine date range
            if not end_date:
                # Search Console has 2-3 day delay, use 3 days ago
                end_date = datetime.now() - timedelta(days=3)

            if not start_date:
                # Get last successful sync
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 16 months of data (Search Console limit)
                    start_date = datetime.now() - timedelta(days=480)

            log.info(f"Syncing Search Console data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            # Sync query performance
            queries_synced = await self._sync_queries(start_date, end_date)
            total_records += queries_synced

            # Sync page performance
            pages_synced = await self._sync_pages(start_date, end_date)
            total_records += pages_synced

            # Sync index coverage (current state, not time-based)
            index_synced = await self._sync_index_coverage()
            total_records += index_synced

            # Sync sitemaps (current state)
            sitemaps_synced = await self._sync_sitemaps()
            total_records += sitemaps_synced

            # Sync rich results (current state)
            rich_results_synced = await self._sync_rich_results()
            total_records += rich_results_synced

            # Calculate sync duration
            sync_duration = time.time() - sync_start_time

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(f"Search Console sync completed: {total_records} records in {sync_duration:.1f}s")

            return {
                "success": True,
                "records_synced": total_records,
                "queries": queries_synced,
                "pages": pages_synced,
                "index_coverage": index_synced,
                "sitemaps": sitemaps_synced,
                "rich_results": rich_results_synced,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"Search Console sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_queries(self, start_date: datetime, end_date: datetime) -> int:
        """Sync query performance data (what users searched for)"""
        try:
            request = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': ['query', 'page', 'device'],
                'rowLimit': 25000  # Max per request
            }

            response = self.service.searchanalytics().query(
                siteUrl=self.site_url,
                body=request
            ).execute()

            records_synced = 0

            if 'rows' in response:
                for row in response['rows']:
                    # Parse dimensions
                    query = row['keys'][0]
                    page = row['keys'][1]
                    device = row['keys'][2]

                    # Create record
                    record = SearchConsoleQuery(
                        date=start_date.date() if start_date.date() == end_date.date() else end_date.date(),
                        query=query,
                        page=page,
                        device=device.upper(),
                        clicks=int(row.get('clicks', 0)),
                        impressions=int(row.get('impressions', 0)),
                        ctr=float(row.get('ctr', 0)),
                        position=float(row.get('position', 0))
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 1000 == 0:
                        self.db.commit()

                self.db.commit()

            log.info(f"Synced {records_synced} Search Console queries")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Search Console queries: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_pages(self, start_date: datetime, end_date: datetime) -> int:
        """Sync page-level performance"""
        try:
            request = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': ['page'],
                'rowLimit': 25000
            }

            response = self.service.searchanalytics().query(
                siteUrl=self.site_url,
                body=request
            ).execute()

            records_synced = 0

            if 'rows' in response:
                for row in response['rows']:
                    page = row['keys'][0]

                    record = SearchConsolePage(
                        date=start_date.date() if start_date.date() == end_date.date() else end_date.date(),
                        page=page,
                        clicks=int(row.get('clicks', 0)),
                        impressions=int(row.get('impressions', 0)),
                        ctr=float(row.get('ctr', 0)),
                        position=float(row.get('position', 0))
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 1000 == 0:
                        self.db.commit()

                self.db.commit()

            log.info(f"Synced {records_synced} Search Console pages")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Search Console pages: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_index_coverage(self) -> int:
        """
        Sync index coverage status

        Note: This requires URL Inspection API which may have limited access.
        Alternatively, use bulk export from Search Console UI.
        For now, we'll try to get sitemap coverage.
        """
        try:
            # Get all sitemaps first
            sitemaps_response = self.service.sitemaps().list(siteUrl=self.site_url).execute()

            records_synced = 0

            if 'sitemap' in sitemaps_response:
                for sitemap in sitemaps_response['sitemap']:
                    sitemap_url = sitemap.get('path')

                    # Get sitemap contents (this gives us some coverage info)
                    if sitemap.get('contents'):
                        for content in sitemap['contents']:
                            # Content type (e.g., web, image, video)
                            content_type = content.get('type', 'unknown')
                            submitted = content.get('submitted', 0)
                            indexed = content.get('indexed', 0)

                            # We don't have individual URLs here, so we'll just log aggregate
                            log.info(f"Sitemap {sitemap_url} - {content_type}: {indexed}/{submitted} indexed")

            # For detailed URL-level coverage, would need URL Inspection API
            # or manual export from Search Console
            log.info(f"Index coverage check completed (aggregate data)")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing index coverage: {str(e)}")
            return 0

    async def _sync_sitemaps(self) -> int:
        """Sync sitemap submission status"""
        try:
            response = self.service.sitemaps().list(siteUrl=self.site_url).execute()

            records_synced = 0

            if 'sitemap' in response:
                for sitemap_data in response['sitemap']:
                    sitemap_url = sitemap_data.get('path')

                    # Parse last submitted date
                    last_submitted = None
                    if 'lastSubmitted' in sitemap_data:
                        last_submitted = datetime.fromisoformat(sitemap_data['lastSubmitted'].replace('Z', '+00:00'))

                    # Count submitted and indexed URLs
                    submitted_urls = 0
                    indexed_urls = 0
                    has_errors = False
                    errors = []

                    if 'contents' in sitemap_data:
                        for content in sitemap_data['contents']:
                            submitted_urls += content.get('submitted', 0)
                            indexed_urls += content.get('indexed', 0)

                    # Check if sitemap has errors
                    if 'errors' in sitemap_data and sitemap_data['errors']:
                        has_errors = True
                        errors = sitemap_data['errors']

                    record = SearchConsoleSitemap(
                        sitemap_url=sitemap_url,
                        submitted_urls=submitted_urls,
                        indexed_urls=indexed_urls,
                        last_submitted=last_submitted,
                        has_errors=has_errors,
                        errors=errors if errors else None
                    )

                    self.db.merge(record)
                    records_synced += 1

                self.db.commit()

            log.info(f"Synced {records_synced} Search Console sitemaps")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Search Console sitemaps: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_rich_results(self) -> int:
        """
        Sync rich results (structured data) status

        Note: Rich Results API access may be limited.
        This would require inspecting specific URLs.
        For now, we'll log that this feature is pending full API access.
        """
        try:
            # Rich Results API is not widely available
            # Would need URL Inspection API to check individual URLs
            # or manual export from Search Console

            log.info("Rich results sync pending (requires URL Inspection API access)")
            return 0

        except Exception as e:
            log.error(f"Error syncing rich results: {str(e)}")
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent query data"""
        try:
            latest = self.db.query(SearchConsoleQuery).order_by(
                desc(SearchConsoleQuery.date)
            ).first()

            if latest:
                return datetime.combine(latest.date, datetime.min.time())

            return None

        except Exception as e:
            log.error(f"Error getting latest Search Console timestamp: {str(e)}")
            return None

    async def backfill(
        self,
        months: int = 16,
        window_days: int = None,
        delay_between_windows: float = None,
        max_retries: int = None
    ) -> Dict[str, Any]:
        """
        Backfill historical Search Console data in chunks to avoid rate limits.

        Args:
            months: Number of months to backfill (max 16, Search Console limit)
            window_days: Size of each fetch window in days (7-30 recommended)
            delay_between_windows: Seconds to wait between windows (rate limit protection)
            max_retries: Max retries per window on failure

        Returns:
            Dict with backfill results including per-window stats
        """
        # Use config defaults if not specified
        if window_days is None:
            window_days = getattr(settings, 'gsc_backfill_window_days', 14)
        if delay_between_windows is None:
            delay_between_windows = getattr(settings, 'gsc_backfill_delay_seconds', 2.0)
        if max_retries is None:
            max_retries = getattr(settings, 'gsc_backfill_max_retries', 3)

        backfill_start = time.time()
        months = min(months, 16)  # Search Console max is 16 months

        # Calculate date range (Search Console has 2-3 day delay)
        end_date = datetime.now() - timedelta(days=3)
        start_date = datetime.now() - timedelta(days=months * 30)

        log.info(f"Starting Search Console backfill: {months} months ({start_date.date()} to {end_date.date()})")

        try:
            # Authenticate if needed
            if not self._authenticated:
                if not await self.authenticate():
                    raise Exception("Authentication failed")

            results = {
                "success": True,
                "months_requested": months,
                "start_date": start_date.date().isoformat(),
                "end_date": end_date.date().isoformat(),
                "window_days": window_days,
                "windows_processed": 0,
                "windows_failed": 0,
                "total_queries": 0,
                "total_pages": 0,
                "window_results": [],
                "errors": []
            }

            # Generate windows
            current_start = start_date
            window_num = 0

            while current_start < end_date:
                window_num += 1
                window_end = min(current_start + timedelta(days=window_days), end_date)

                log.info(f"Processing window {window_num}: {current_start.date()} to {window_end.date()}")

                # Try to sync this window with retries
                window_result = await self._sync_window_with_retry(
                    current_start, window_end, max_retries
                )

                results["window_results"].append({
                    "window": window_num,
                    "start_date": current_start.date().isoformat(),
                    "end_date": window_end.date().isoformat(),
                    "success": window_result["success"],
                    "queries": window_result.get("queries", 0),
                    "pages": window_result.get("pages", 0),
                    "error": window_result.get("error")
                })

                if window_result["success"]:
                    results["windows_processed"] += 1
                    results["total_queries"] += window_result.get("queries", 0)
                    results["total_pages"] += window_result.get("pages", 0)
                else:
                    results["windows_failed"] += 1
                    results["errors"].append({
                        "window": window_num,
                        "dates": f"{current_start.date()} to {window_end.date()}",
                        "error": window_result.get("error", "Unknown error")
                    })

                # Move to next window
                current_start = window_end + timedelta(days=1)

                # Rate limit delay between windows (skip on last window)
                if current_start < end_date and delay_between_windows > 0:
                    log.debug(f"Waiting {delay_between_windows}s before next window...")
                    await asyncio.sleep(delay_between_windows)

            results["duration_seconds"] = round(time.time() - backfill_start, 2)
            results["success"] = results["windows_failed"] == 0

            log.info(
                f"Search Console backfill complete: "
                f"{results['windows_processed']}/{window_num} windows, "
                f"{results['total_queries']} queries, "
                f"{results['total_pages']} pages in {results['duration_seconds']}s"
            )

            return results

        except Exception as e:
            error_msg = f"Search Console backfill failed: {str(e)}"
            log.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "duration_seconds": round(time.time() - backfill_start, 2)
            }

    async def _sync_window_with_retry(
        self,
        start_date: datetime,
        end_date: datetime,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Sync a single date window with retry logic.

        Args:
            start_date: Window start
            end_date: Window end
            max_retries: Maximum retry attempts

        Returns:
            Dict with window sync results
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                queries_synced = await self._sync_queries(start_date, end_date)
                pages_synced = await self._sync_pages(start_date, end_date)

                return {
                    "success": True,
                    "queries": queries_synced,
                    "pages": pages_synced,
                    "attempts": attempt + 1
                }

            except HttpError as e:
                last_error = str(e)
                if e.resp.status == 429:  # Rate limited
                    delay = (2 ** attempt) * 5  # Exponential backoff: 5, 10, 20 seconds
                    log.warning(f"Rate limited on window sync, waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                elif e.resp.status >= 500:  # Server error
                    delay = (2 ** attempt) * 2
                    log.warning(f"Server error on window sync, waiting {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    # Non-retryable error
                    log.error(f"Non-retryable error syncing window: {e}")
                    break

            except Exception as e:
                last_error = str(e)
                log.error(f"Error syncing window (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        return {
            "success": False,
            "error": last_error,
            "attempts": max_retries
        }

    async def daily_sync(self, days: int = None) -> Dict[str, Any]:
        """
        Daily incremental sync for recent data.

        Args:
            days: Number of days to sync (1-7, accounts for Search Console data delay)

        Returns:
            Dict with sync results
        """
        # Use config default if not specified
        if days is None:
            days = getattr(settings, 'gsc_daily_sync_days', 3)
        days = min(max(days, 1), 7)  # Clamp to 1-7 days

        # Account for Search Console's 2-3 day data delay
        end_date = datetime.now() - timedelta(days=3)
        start_date = end_date - timedelta(days=days)

        log.info(f"Starting Search Console daily sync: {start_date.date()} to {end_date.date()}")

        try:
            # Authenticate if needed
            if not self._authenticated:
                if not await self.authenticate():
                    raise Exception("Authentication failed")

            sync_start = time.time()

            # Log sync start
            await self.log_sync_start()

            # Sync queries and pages
            queries_synced = await self._sync_queries(start_date, end_date)
            pages_synced = await self._sync_pages(start_date, end_date)

            # Also sync current state data
            sitemaps_synced = await self._sync_sitemaps()

            total_records = queries_synced + pages_synced + sitemaps_synced
            duration = round(time.time() - sync_start, 2)

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=duration
            )

            result = {
                "success": True,
                "sync_type": "daily",
                "days_synced": days,
                "start_date": start_date.date().isoformat(),
                "end_date": end_date.date().isoformat(),
                "queries": queries_synced,
                "pages": pages_synced,
                "sitemaps": sitemaps_synced,
                "total_records": total_records,
                "duration_seconds": duration
            }

            log.info(f"Search Console daily sync complete: {total_records} records in {duration}s")
            return result

        except Exception as e:
            error_msg = f"Search Console daily sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "sync_type": "daily",
                "error": error_msg
            }
