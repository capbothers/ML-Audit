"""
Google Search Console data connector (lightweight version)
Fetches query performance, page rankings, and SEO data
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import time
import asyncio
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class SearchConsoleConnector(BaseConnector):
    """Connector for Google Search Console API"""

    def __init__(self):
        super().__init__("Google Search Console")
        self.credentials_path = settings.gsc_credentials_path
        self.site_url = settings.gsc_site_url
        self.service = None

    async def connect(self) -> bool:
        """Establish connection to Search Console"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/webmasters.readonly']
            )
            self.service = build('searchconsole', 'v1', credentials=credentials)
            log.info(f"Connected to Google Search Console: {self.site_url}")
            return True
        except Exception as e:
            log.error(f"Failed to connect to Search Console: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate Search Console connection"""
        try:
            if not self.service:
                await self.connect()

            # Test with site list
            site_list = self.service.sites().list().execute()
            return True
        except Exception as e:
            log.error(f"Search Console connection validation failed: {str(e)}")
            return False

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive Search Console data"""
        if not self.service:
            await self.connect()

        # Search Console data has 2-3 day delay
        # Use timezone-aware datetime to match end_date
        now = datetime.now(end_date.tzinfo) if end_date.tzinfo else datetime.now()
        adjusted_end = min(end_date, now - timedelta(days=3))

        data = {
            "query_performance": await self._fetch_query_performance(start_date, adjusted_end),
            "page_performance": await self._fetch_page_performance(start_date, adjusted_end),
            "device_breakdown": await self._fetch_device_breakdown(start_date, adjusted_end),
            "country_breakdown": await self._fetch_country_breakdown(start_date, adjusted_end),
            "sitemaps": await self._fetch_sitemaps(),
        }
        return data

    async def _fetch_query_performance(self, start_date: datetime, end_date: datetime) -> Dict:
        """Fetch top queries with their performance"""
        try:
            all_queries = []
            start_row = 0
            row_limit = 25000

            while True:
                request = {
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d'),
                    'dimensions': ['date', 'query'],  # Include date for per-day granularity
                    'rowLimit': row_limit,
                    'startRow': start_row
                }

                response = self.service.searchanalytics().query(
                    siteUrl=self.site_url,
                    body=request
                ).execute()

                rows = response.get('rows', [])
                if not rows:
                    break

                for row in rows:
                    all_queries.append({
                        "date": row['keys'][0],  # First key is date
                        "query": row['keys'][1],  # Second key is query
                        "clicks": int(row.get('clicks', 0)),
                        "impressions": int(row.get('impressions', 0)),
                        "ctr": round(float(row.get('ctr', 0)), 6),  # Decimal 0-1
                        "position": round(float(row.get('position', 0)), 1),
                    })

                log.info(f"Fetched {len(all_queries)} queries from Search Console...")

                if len(rows) < row_limit:
                    break
                start_row += row_limit

            # Calculate totals
            total_clicks = sum(q['clicks'] for q in all_queries)
            total_impressions = sum(q['impressions'] for q in all_queries)

            log.info(f"Fetched {len(all_queries)} total queries from Search Console")
            return {
                "queries": all_queries,  # Return all queries (date dimension means we need full coverage)
                "total_queries": len(all_queries),
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "avg_ctr": round((total_clicks / total_impressions) if total_impressions > 0 else 0, 6),
            }

        except Exception as e:
            log.error(f"Error fetching Search Console queries: {str(e)}")
            return {"queries": [], "total_queries": 0}

    async def _fetch_page_performance(self, start_date: datetime, end_date: datetime) -> Dict:
        """Fetch page-level performance"""
        try:
            all_pages = []
            start_row = 0
            row_limit = 25000

            while True:
                request = {
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d'),
                    'dimensions': ['date', 'page'],  # Include date for per-day granularity
                    'rowLimit': row_limit,
                    'startRow': start_row
                }

                response = self.service.searchanalytics().query(
                    siteUrl=self.site_url,
                    body=request
                ).execute()

                rows = response.get('rows', [])
                if not rows:
                    break

                for row in rows:
                    all_pages.append({
                        "date": row['keys'][0],  # First key is date
                        "page": row['keys'][1],  # Second key is page
                        "clicks": int(row.get('clicks', 0)),
                        "impressions": int(row.get('impressions', 0)),
                        "ctr": round(float(row.get('ctr', 0)), 6),  # Decimal 0-1
                        "position": round(float(row.get('position', 0)), 1),
                    })

                log.info(f"Fetched {len(all_pages)} pages from Search Console...")

                if len(rows) < row_limit:
                    break
                start_row += row_limit

            log.info(f"Fetched {len(all_pages)} total pages from Search Console")
            return {
                "pages": all_pages,  # Return all pages (date dimension means we need full coverage)
                "total_pages": len(all_pages),
            }

        except Exception as e:
            log.error(f"Error fetching Search Console pages: {str(e)}")
            return {"pages": [], "total_pages": 0}

    async def _fetch_device_breakdown(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch performance by device"""
        try:
            request = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': ['device'],
                'rowLimit': 10
            }

            response = self.service.searchanalytics().query(
                siteUrl=self.site_url,
                body=request
            ).execute()

            devices = []
            for row in response.get('rows', []):
                devices.append({
                    "device": row['keys'][0],
                    "clicks": int(row.get('clicks', 0)),
                    "impressions": int(row.get('impressions', 0)),
                    "ctr": round(float(row.get('ctr', 0)), 6),  # Decimal 0-1
                    "position": round(float(row.get('position', 0)), 1),
                })

            log.info(f"Fetched device breakdown from Search Console")
            return devices

        except Exception as e:
            log.error(f"Error fetching Search Console device breakdown: {str(e)}")
            return []

    async def _fetch_country_breakdown(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch performance by country"""
        try:
            request = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'dimensions': ['country'],
                'rowLimit': 50
            }

            response = self.service.searchanalytics().query(
                siteUrl=self.site_url,
                body=request
            ).execute()

            countries = []
            for row in response.get('rows', []):
                countries.append({
                    "country": row['keys'][0],
                    "clicks": int(row.get('clicks', 0)),
                    "impressions": int(row.get('impressions', 0)),
                    "ctr": round(float(row.get('ctr', 0)), 6),  # Decimal 0-1
                    "position": round(float(row.get('position', 0)), 1),
                })

            log.info(f"Fetched {len(countries)} countries from Search Console")
            return countries

        except Exception as e:
            log.error(f"Error fetching Search Console country breakdown: {str(e)}")
            return []

    async def _fetch_sitemaps(self) -> List[Dict]:
        """Fetch sitemap status with all available fields"""
        try:
            response = self.service.sitemaps().list(siteUrl=self.site_url).execute()

            sitemaps = []
            for sitemap in response.get('sitemap', []):
                # Aggregate submitted/indexed from contents
                submitted = 0
                indexed = 0
                for content in sitemap.get('contents', []):
                    submitted += content.get('submitted', 0)
                    indexed += content.get('indexed', 0)

                sitemaps.append({
                    "url": sitemap.get('path'),
                    "submitted_urls": submitted,
                    "indexed_urls": indexed,
                    "last_submitted": sitemap.get('lastSubmitted'),
                    "last_downloaded": sitemap.get('lastDownloaded'),
                    "is_pending": sitemap.get('isPending', False),
                    "is_sitemaps_index": sitemap.get('isSitemapsIndex', False),
                    "errors": sitemap.get('errors', 0),
                    "warnings": sitemap.get('warnings', 0),
                })

            log.info(f"Fetched {len(sitemaps)} sitemaps from Search Console")
            return sitemaps

        except Exception as e:
            log.error(f"Error fetching Search Console sitemaps: {str(e)}")
            return []

    async def fetch_quick(self) -> Dict[str, Any]:
        """Quick fetch - summary data only (last 7 days)"""
        if not self.service:
            await self.connect()

        end_date = datetime.now() - timedelta(days=3)
        start_date = end_date - timedelta(days=7)

        data = {
            "query_performance": await self._fetch_query_performance(start_date, end_date),
            "device_breakdown": await self._fetch_device_breakdown(start_date, end_date),
            "sitemaps": await self._fetch_sitemaps(),
        }
        return data

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
            # Connect if needed
            if not self.service:
                await self.connect()

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
        Returns dict with query and page performance data.
        """
        last_error = None

        for attempt in range(max_retries):
            try:
                # Fetch query and page performance for this window
                query_result = await self._fetch_query_performance(start_date, end_date)
                page_result = await self._fetch_page_performance(start_date, end_date)

                return {
                    "success": True,
                    "queries": query_result.get("total_queries", 0),
                    "pages": page_result.get("total_pages", 0),
                    "data": {
                        "query_performance": query_result,
                        "page_performance": page_result,
                        "start_date": start_date.strftime('%Y-%m-%d'),
                        "end_date": end_date.strftime('%Y-%m-%d')
                    },
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
            # Connect if needed
            if not self.service:
                await self.connect()

            sync_start = time.time()

            # Fetch data
            query_result = await self._fetch_query_performance(start_date, end_date)
            page_result = await self._fetch_page_performance(start_date, end_date)
            sitemaps = await self._fetch_sitemaps()

            total_queries = query_result.get("total_queries", 0)
            total_pages = page_result.get("total_pages", 0)
            total_records = total_queries + total_pages + len(sitemaps)

            duration = round(time.time() - sync_start, 2)

            result = {
                "success": True,
                "sync_type": "daily",
                "days_synced": days,
                "start_date": start_date.date().isoformat(),
                "end_date": end_date.date().isoformat(),
                "queries": total_queries,
                "pages": total_pages,
                "sitemaps": len(sitemaps),
                "total_records": total_records,
                "duration_seconds": duration,
                "data": {
                    "query_performance": query_result,
                    "page_performance": page_result,
                    "sitemaps": sitemaps
                }
            }

            log.info(f"Search Console daily sync complete: {total_records} records in {duration}s")
            return result

        except Exception as e:
            error_msg = f"Search Console daily sync failed: {str(e)}"
            log.error(error_msg)

            return {
                "success": False,
                "sync_type": "daily",
                "error": error_msg
            }
