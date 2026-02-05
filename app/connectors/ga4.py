"""
Google Analytics 4 Connector (DEPRECATED)

DEPRECATED: Use ga4_connector.py with DataSyncService instead.

This connector saves data directly to the database. The preferred pattern is to use
ga4_connector.py which returns data to DataSyncService for persistence, enabling
sync logging, validation, and consistent error handling.

Syncs traffic sources, landing pages, products, and conversion paths from GA4.
"""
import warnings

warnings.warn(
    "ga4.py is deprecated. Use ga4_connector.py with DataSyncService instead.",
    DeprecationWarning,
    stacklevel=2
)
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Metric, RunReportRequest
    )
    from google.oauth2 import service_account
except ImportError:
    BetaAnalyticsDataClient = None
    service_account = None

from app.connectors.base import BaseConnector
from app.models.ga4_data import (
    GA4TrafficSource, GA4LandingPage, GA4ProductPerformance,
    GA4ConversionPath, GA4Event, GA4PagePerformance
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class GA4Connector(BaseConnector):
    """
    Google Analytics 4 Data API connector

    Syncs:
    - Traffic by source/medium/campaign (daily)
    - Landing page performance (daily)
    - E-commerce product performance
    - Conversion paths (for attribution)
    - Events
    - Page performance
    """

    def __init__(self, db: Session):
        super().__init__(db, source_name="ga4", source_type="analytics")
        self.client = None
        self.property_id = settings.ga4_property_id

    async def authenticate(self) -> bool:
        """Authenticate with GA4 Data API using service account"""
        try:
            if not BetaAnalyticsDataClient:
                log.error("GA4 SDK not installed. Install with: pip install google-analytics-data")
                return False

            if not settings.ga4_credentials_path or not settings.ga4_property_id:
                log.error("Missing GA4 credentials in settings")
                return False

            # Load credentials from JSON file
            credentials = service_account.Credentials.from_service_account_file(
                settings.ga4_credentials_path,
                scopes=["https://www.googleapis.com/auth/analytics.readonly"]
            )

            # Initialize client
            self.client = BetaAnalyticsDataClient(credentials=credentials)

            # Test with simple query
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name="sessions")],
                limit=1
            )

            response = self.client.run_report(request)

            self._authenticated = True
            log.info("GA4 authentication successful")
            return True

        except Exception as e:
            log.error(f"GA4 authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Sync data from GA4"""
        sync_start_time = time.time()

        try:
            if not self._authenticated:
                if not await self.authenticate():
                    raise Exception("Authentication failed")

            await self.log_sync_start()

            # GA4 has 24-48 hour delay, use 2 days ago as end date
            if not end_date:
                end_date = datetime.now() - timedelta(days=2)

            if not start_date:
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 1 year of data (GA4 historical limit varies)
                    start_date = datetime.now() - timedelta(days=365)

            log.info(f"Syncing GA4 data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            # Sync traffic sources
            traffic_synced = await self._sync_traffic_sources(start_date, end_date)
            total_records += traffic_synced

            # Sync landing pages
            pages_synced = await self._sync_landing_pages(start_date, end_date)
            total_records += pages_synced

            # Sync product performance
            products_synced = await self._sync_products(start_date, end_date)
            total_records += products_synced

            # Sync page performance
            page_perf_synced = await self._sync_page_performance(start_date, end_date)
            total_records += page_perf_synced

            sync_duration = time.time() - sync_start_time

            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            return {
                "success": True,
                "records_synced": total_records,
                "traffic_sources": traffic_synced,
                "landing_pages": pages_synced,
                "products": products_synced,
                "page_performance": page_perf_synced,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"GA4 sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_traffic_sources(self, start_date: datetime, end_date: datetime) -> int:
        """Sync traffic by source/medium/campaign"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionCampaignName")
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="newUsers"),
                    Metric(name="engagedSessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue")
                ]
            )

            response = self.client.run_report(request)

            records_synced = 0

            for row in response.rows:
                date_str = row.dimension_values[0].value
                source = row.dimension_values[1].value
                medium = row.dimension_values[2].value
                campaign = row.dimension_values[3].value

                record = GA4TrafficSource(
                    date=datetime.strptime(date_str, '%Y%m%d').date(),
                    session_source=source if source != "(not set)" else None,
                    session_medium=medium if medium != "(not set)" else None,
                    session_campaign_name=campaign if campaign != "(not set)" else None,
                    sessions=int(row.metric_values[0].value),
                    total_users=int(row.metric_values[1].value),
                    new_users=int(row.metric_values[2].value),
                    engaged_sessions=int(row.metric_values[3].value),
                    bounce_rate=float(row.metric_values[4].value),
                    avg_session_duration=float(row.metric_values[5].value),
                    conversions=int(float(row.metric_values[6].value)),
                    total_revenue=float(row.metric_values[7].value)
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} GA4 traffic sources")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GA4 traffic sources: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_landing_pages(self, start_date: datetime, end_date: datetime) -> int:
        """Sync landing page performance"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="landingPage"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium")
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue")
                ]
            )

            response = self.client.run_report(request)

            records_synced = 0

            for row in response.rows:
                date_str = row.dimension_values[0].value
                landing_page = row.dimension_values[1].value
                source = row.dimension_values[2].value
                medium = row.dimension_values[3].value

                sessions = int(row.metric_values[0].value)
                conversions = int(float(row.metric_values[3].value))

                record = GA4LandingPage(
                    date=datetime.strptime(date_str, '%Y%m%d').date(),
                    landing_page=landing_page,
                    session_source=source if source != "(not set)" else None,
                    session_medium=medium if medium != "(not set)" else None,
                    sessions=sessions,
                    bounce_rate=float(row.metric_values[1].value),
                    avg_session_duration=float(row.metric_values[2].value),
                    conversions=conversions,
                    conversion_rate=conversions / sessions if sessions > 0 else 0,
                    total_revenue=float(row.metric_values[4].value)
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} GA4 landing pages")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GA4 landing pages: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_products(self, start_date: datetime, end_date: datetime) -> int:
        """Sync e-commerce product performance"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="itemId"),
                    Dimension(name="itemName"),
                    Dimension(name="itemCategory")
                ],
                metrics=[
                    Metric(name="itemsViewed"),
                    Metric(name="itemsAddedToCart"),
                    Metric(name="itemsPurchased"),
                    Metric(name="itemRevenue")
                ]
            )

            response = self.client.run_report(request)

            records_synced = 0

            for row in response.rows:
                date_str = row.dimension_values[0].value
                item_id = row.dimension_values[1].value
                item_name = row.dimension_values[2].value
                item_category = row.dimension_values[3].value

                items_viewed = int(float(row.metric_values[0].value))
                items_added = int(float(row.metric_values[1].value))

                record = GA4ProductPerformance(
                    date=datetime.strptime(date_str, '%Y%m%d').date(),
                    item_id=item_id,
                    item_name=item_name if item_name != "(not set)" else None,
                    item_category=item_category if item_category != "(not set)" else None,
                    items_viewed=items_viewed,
                    items_added_to_cart=items_added,
                    items_purchased=int(float(row.metric_values[2].value)),
                    item_revenue=float(row.metric_values[3].value),
                    add_to_cart_rate=items_added / items_viewed if items_viewed > 0 else 0
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} GA4 products")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GA4 products: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_page_performance(self, start_date: datetime, end_date: datetime) -> int:
        """Sync page-level performance"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="pagePath"),
                    Dimension(name="pageTitle")
                ],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration")
                ]
            )

            response = self.client.run_report(request)

            records_synced = 0

            for row in response.rows:
                date_str = row.dimension_values[0].value
                page_path = row.dimension_values[1].value
                page_title = row.dimension_values[2].value

                pageviews = int(row.metric_values[0].value)
                sessions = int(row.metric_values[1].value)

                record = GA4PagePerformance(
                    date=datetime.strptime(date_str, '%Y%m%d').date(),
                    page_path=page_path,
                    page_title=page_title if page_title != "(not set)" else None,
                    pageviews=pageviews,
                    unique_pageviews=sessions,  # Approximation
                    entrances=sessions,  # Approximation
                    bounce_rate=float(row.metric_values[2].value),
                    avg_time_on_page=float(row.metric_values[3].value)
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} GA4 pages")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing GA4 pages: {str(e)}")
            self.db.rollback()
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent traffic data"""
        try:
            latest = self.db.query(GA4TrafficSource).order_by(
                desc(GA4TrafficSource.date)
            ).first()

            if latest:
                return datetime.combine(latest.date, datetime.min.time())

            return None

        except Exception as e:
            log.error(f"Error getting latest GA4 timestamp: {str(e)}")
            return None
