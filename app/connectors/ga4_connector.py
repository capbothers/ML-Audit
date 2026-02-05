"""
Google Analytics 4 data connector
Fetches website analytics, user behavior, and conversion data

This is the primary GA4 connector used by DataSyncService.
"""
from typing import Any, Dict, List
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()

# GA4 has a 24-48 hour data processing delay
GA4_DATA_DELAY_DAYS = 2


class GA4Connector(BaseConnector):
    """Connector for Google Analytics 4"""

    def __init__(self):
        super().__init__("Google Analytics 4")
        self.property_id = settings.ga4_property_id
        self.client = None

    async def connect(self) -> bool:
        """Establish connection to GA4"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                settings.ga4_credentials_path
            )
            self.client = BetaAnalyticsDataClient(credentials=credentials)
            log.info("Connected to Google Analytics 4")
            return True
        except Exception as e:
            log.error(f"Failed to connect to GA4: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate GA4 connection"""
        try:
            if not self.client:
                await self.connect()

            # Test with a simple request
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
                metrics=[Metric(name="activeUsers")],
            )
            self.client.run_report(request)
            return True
        except Exception as e:
            log.error(f"GA4 connection validation failed: {str(e)}")
            return False

    async def sync(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Sync GA4 data for the given date range.

        Note: GA4 has a 24-48 hour data processing delay. This method automatically
        adjusts the end_date to account for this delay.

        Returns:
            Dict with success status and data
        """
        if not self.client:
            connected = await self.connect()
            if not connected:
                return {"success": False, "error": "Failed to connect to GA4"}

        # Adjust end_date to account for GA4 data delay
        # Data from the last 2 days may be incomplete
        # Use timezone-aware now() matching end_date's tzinfo to avoid comparison error
        now = datetime.now(end_date.tzinfo) if end_date.tzinfo else datetime.now()
        adjusted_end_date = min(
            end_date,
            now - timedelta(days=GA4_DATA_DELAY_DAYS)
        )

        # Ensure start_date is not after adjusted end_date
        if start_date > adjusted_end_date:
            log.warning(
                f"GA4: Start date {start_date.date()} is after adjusted end date "
                f"{adjusted_end_date.date()} (due to {GA4_DATA_DELAY_DAYS}-day data delay). "
                "No data to fetch."
            )
            return {
                "success": True,
                "data": {},
                "message": f"No data available yet (GA4 has {GA4_DATA_DELAY_DAYS}-day processing delay)"
            }

        log.info(
            f"GA4 sync: {start_date.date()} to {adjusted_end_date.date()} "
            f"(adjusted from {end_date.date()} for {GA4_DATA_DELAY_DAYS}-day data delay)"
        )

        try:
            data = await self.fetch_data(start_date, adjusted_end_date)
            return {
                "success": True,
                "data": data,
                "date_range": {
                    "requested_start": start_date.isoformat(),
                    "requested_end": end_date.isoformat(),
                    "actual_start": start_date.isoformat(),
                    "actual_end": adjusted_end_date.isoformat(),
                    "data_delay_days": GA4_DATA_DELAY_DAYS
                }
            }
        except Exception as e:
            log.error(f"GA4 sync failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive GA4 data with all metrics and segmentation."""
        if not self.client:
            await self.connect()

        data = {
            # Phase 1: Core daily metrics
            "daily_summary": await self._fetch_daily_summary(start_date, end_date),
            "conversions": await self._fetch_conversions(start_date, end_date),
            "ecommerce": await self._fetch_ecommerce_metrics(start_date, end_date),

            # Traffic sources with pagination for large datasets
            "traffic_sources": await self._fetch_traffic_sources_paginated(start_date, end_date),
            "landing_pages": await self._fetch_landing_pages_paginated(start_date, end_date),
            "pages": await self._fetch_page_performance_paginated(start_date, end_date),
            "products": await self._fetch_product_performance_paginated(start_date, end_date),

            # Phase 2: Advanced segmentation
            "device_breakdown": await self._fetch_device_breakdown(start_date, end_date),
            "geo_breakdown": await self._fetch_geo_breakdown(start_date, end_date, granularity="country"),
            "user_type_breakdown": await self._fetch_user_type_breakdown(start_date, end_date),
        }
        return data

    def _format_date(self, date: datetime) -> str:
        """Format datetime to GA4 date string"""
        return date.strftime("%Y-%m-%d")

    async def _fetch_traffic_overview(self, start_date: datetime, end_date: datetime) -> Dict:
        """Fetch overall traffic metrics"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="newUsers"),
                    Metric(name="sessions"),
                    Metric(name="screenPageViews"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="bounceRate"),
                ],
                dimensions=[Dimension(name="date")],
            )

            response = self.client.run_report(request)

            daily_metrics = []
            for row in response.rows:
                daily_metrics.append({
                    "date": row.dimension_values[0].value,
                    "active_users": int(row.metric_values[0].value),
                    "new_users": int(row.metric_values[1].value),
                    "sessions": int(row.metric_values[2].value),
                    "pageviews": int(row.metric_values[3].value),
                    "avg_session_duration": float(row.metric_values[4].value),
                    "bounce_rate": float(row.metric_values[5].value),
                })

            log.info(f"Fetched {len(daily_metrics)} days of traffic data from GA4")
            return {"daily_metrics": daily_metrics}

        except Exception as e:
            log.error(f"Error fetching GA4 traffic overview: {str(e)}")
            return {}

    async def _fetch_user_acquisition(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch user acquisition data by source/medium"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="newUsers"),
                    Metric(name="sessions"),
                    Metric(name="engagementRate"),
                ],
                dimensions=[
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                ],
            )

            response = self.client.run_report(request)

            acquisitions = []
            for row in response.rows:
                acquisitions.append({
                    "source": row.dimension_values[0].value,
                    "medium": row.dimension_values[1].value,
                    "new_users": int(row.metric_values[0].value),
                    "sessions": int(row.metric_values[1].value),
                    "engagement_rate": float(row.metric_values[2].value),
                })

            log.info(f"Fetched {len(acquisitions)} acquisition channels from GA4")
            return acquisitions

        except Exception as e:
            log.error(f"Error fetching GA4 user acquisition: {str(e)}")
            return []

    async def _fetch_engagement_metrics(self, start_date: datetime, end_date: datetime) -> Dict:
        """Fetch user engagement metrics"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="engagedSessions"),
                    Metric(name="engagementRate"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="eventCount"),
                ],
            )

            response = self.client.run_report(request)

            if response.rows:
                row = response.rows[0]
                return {
                    "engaged_sessions": int(row.metric_values[0].value),
                    "engagement_rate": float(row.metric_values[1].value),
                    "total_engagement_duration": float(row.metric_values[2].value),
                    "event_count": int(row.metric_values[3].value),
                }

            return {}

        except Exception as e:
            log.error(f"Error fetching GA4 engagement metrics: {str(e)}")
            return {}

    async def _fetch_conversions(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch conversion events with date dimension for per-day storage"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="eventCount"),
                    Metric(name="totalUsers"),
                    Metric(name="totalRevenue"),
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="eventName"),
                ],
            )

            response = self.client.run_report(request)

            conversions = []
            for row in response.rows:
                conversions.append({
                    "date": row.dimension_values[0].value,
                    "event_name": row.dimension_values[1].value,
                    "event_count": int(row.metric_values[0].value),
                    "total_users": int(row.metric_values[1].value),
                    "revenue": float(row.metric_values[2].value),
                })

            log.info(f"Fetched {len(conversions)} conversion event records from GA4")
            return conversions

        except Exception as e:
            log.error(f"Error fetching GA4 conversions: {str(e)}")
            return []

    async def _fetch_ecommerce_metrics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch daily e-commerce metrics with date dimension.

        Uses ecommercePurchases (not conversions) for accurate Shopify reconciliation.
        The conversions metric includes all conversion events, not just purchases.
        """
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="ecommercePurchases"),
                    Metric(name="totalRevenue"),
                    Metric(name="addToCarts"),
                    Metric(name="checkouts"),
                    Metric(name="itemsViewed"),
                ],
                dimensions=[
                    Dimension(name="date"),
                ],
            )

            response = self.client.run_report(request)

            daily_ecommerce = []
            for row in response.rows:
                purchases = int(row.metric_values[0].value)
                add_to_carts = int(row.metric_values[2].value)
                daily_ecommerce.append({
                    "date": row.dimension_values[0].value,
                    "ecommerce_purchases": purchases,
                    "revenue": float(row.metric_values[1].value),
                    "add_to_carts": add_to_carts,
                    "checkouts": int(row.metric_values[3].value),
                    "items_viewed": int(row.metric_values[4].value),
                    "cart_to_purchase_rate": purchases / add_to_carts if add_to_carts > 0 else 0,
                })

            log.info(f"Fetched {len(daily_ecommerce)} daily ecommerce records from GA4")
            return daily_ecommerce

        except Exception as e:
            log.error(f"Error fetching GA4 ecommerce metrics: {str(e)}")
            return []

    async def _fetch_page_performance(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch page-level performance data with date dimension"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="screenPageViews"),
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="pagePath"),
                    Dimension(name="pageTitle"),
                ],
                # No row limit - fetch all pages
            )

            response = self.client.run_report(request)

            pages = []
            for row in response.rows:
                pageviews = int(row.metric_values[0].value)
                sessions = int(row.metric_values[1].value)
                pages.append({
                    "date": row.dimension_values[0].value,
                    "path": row.dimension_values[1].value,
                    "title": row.dimension_values[2].value,
                    "pageviews": pageviews,
                    "sessions": sessions,
                    "bounce_rate": float(row.metric_values[2].value),
                    "avg_time_on_page": float(row.metric_values[3].value),
                })

            log.info(f"Fetched {len(pages)} page records from GA4")
            return pages

        except Exception as e:
            log.error(f"Error fetching GA4 page performance: {str(e)}")
            return []

    async def _fetch_traffic_sources(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch traffic source breakdown with date dimension for per-day storage"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="totalUsers"),
                    Metric(name="newUsers"),
                    Metric(name="engagedSessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                    Dimension(name="sessionCampaignName"),
                ],
            )

            response = self.client.run_report(request)

            sources = []
            for row in response.rows:
                sources.append({
                    "date": row.dimension_values[0].value,
                    "source": row.dimension_values[1].value,
                    "medium": row.dimension_values[2].value,
                    "campaign": row.dimension_values[3].value,
                    "sessions": int(row.metric_values[0].value),
                    "total_users": int(row.metric_values[1].value),
                    "new_users": int(row.metric_values[2].value),
                    "engaged_sessions": int(row.metric_values[3].value),
                    "bounce_rate": float(row.metric_values[4].value),
                    "avg_session_duration": float(row.metric_values[5].value),
                    "conversions": int(float(row.metric_values[6].value)),
                    "revenue": float(row.metric_values[7].value),
                })

            log.info(f"Fetched {len(sources)} traffic source records from GA4")
            return sources

        except Exception as e:
            log.error(f"Error fetching GA4 traffic sources: {str(e)}")
            return []

    async def _fetch_landing_pages(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch landing page performance with date dimension"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="landingPage"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium"),
                ],
            )

            response = self.client.run_report(request)

            landing_pages = []
            for row in response.rows:
                sessions = int(row.metric_values[0].value)
                conversions = int(float(row.metric_values[3].value))
                landing_pages.append({
                    "date": row.dimension_values[0].value,
                    "landing_page": row.dimension_values[1].value,
                    "source": row.dimension_values[2].value,
                    "medium": row.dimension_values[3].value,
                    "sessions": sessions,
                    "bounce_rate": float(row.metric_values[1].value),
                    "avg_session_duration": float(row.metric_values[2].value),
                    "conversions": conversions,
                    "conversion_rate": conversions / sessions if sessions > 0 else 0,
                    "revenue": float(row.metric_values[4].value),
                })

            log.info(f"Fetched {len(landing_pages)} landing page records from GA4")
            return landing_pages

        except Exception as e:
            log.error(f"Error fetching GA4 landing pages: {str(e)}")
            return []

    async def _fetch_product_performance(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch e-commerce product performance with date dimension"""
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                metrics=[
                    Metric(name="itemsViewed"),
                    Metric(name="itemsAddedToCart"),
                    Metric(name="itemsPurchased"),
                    Metric(name="itemRevenue"),
                ],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="itemId"),
                    Dimension(name="itemName"),
                    Dimension(name="itemCategory"),
                ],
            )

            response = self.client.run_report(request)

            products = []
            for row in response.rows:
                items_viewed = int(float(row.metric_values[0].value))
                items_added = int(float(row.metric_values[1].value))
                products.append({
                    "date": row.dimension_values[0].value,
                    "item_id": row.dimension_values[1].value,
                    "item_name": row.dimension_values[2].value,
                    "item_category": row.dimension_values[3].value,
                    "items_viewed": items_viewed,
                    "items_added_to_cart": items_added,
                    "items_purchased": int(float(row.metric_values[2].value)),
                    "item_revenue": float(row.metric_values[3].value),
                    "add_to_cart_rate": items_added / items_viewed if items_viewed > 0 else 0,
                })

            log.info(f"Fetched {len(products)} product records from GA4")
            return products

        except Exception as e:
            log.error(f"Error fetching GA4 product performance: {str(e)}")
            return []

    async def _fetch_daily_summary(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch comprehensive daily site-wide metrics.

        Returns one record per day with all key metrics including:
        - Traffic: users, sessions, pageviews
        - Engagement: bounce rate, session duration, engagement rate
        - Conversions: total conversions and revenue

        Note: GA4 limits requests to 10 metrics, so this makes two requests
        and merges the results by date.
        """
        try:
            # Request A: Traffic + Engagement (10 metrics - GA4 limit)
            request_a = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                dimensions=[Dimension(name="date")],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="newUsers"),
                    Metric(name="sessions"),
                    Metric(name="screenPageViews"),
                    Metric(name="engagedSessions"),
                    Metric(name="engagementRate"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="userEngagementDuration"),
                    Metric(name="eventCount"),
                ],
            )

            response_a = self.client.run_report(request_a)

            # Request B: Conversions + Revenue (2 metrics)
            request_b = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                dimensions=[Dimension(name="date")],
                metrics=[
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
            )

            response_b = self.client.run_report(request_b)

            # Build lookup from Request B by date
            conversion_data = {}
            for row in response_b.rows:
                date_val = row.dimension_values[0].value
                conversion_data[date_val] = {
                    "total_conversions": int(float(row.metric_values[0].value)),
                    "total_revenue": float(row.metric_values[1].value),
                }

            # Merge results from Request A with Request B
            daily_data = []
            for row in response_a.rows:
                date_val = row.dimension_values[0].value
                active_users = int(row.metric_values[0].value)
                new_users = int(row.metric_values[1].value)
                sessions = int(row.metric_values[2].value)
                pageviews = int(row.metric_values[3].value)
                total_events = int(row.metric_values[9].value)
                user_engagement_duration = float(row.metric_values[8].value)

                # Get conversion data for this date (default to 0 if missing)
                conv = conversion_data.get(date_val, {"total_conversions": 0, "total_revenue": 0.0})

                daily_data.append({
                    "date": date_val,
                    "active_users": active_users,
                    "new_users": new_users,
                    "returning_users": max(0, active_users - new_users),
                    "sessions": sessions,
                    "pageviews": pageviews,
                    "engaged_sessions": int(row.metric_values[4].value),
                    "engagement_rate": float(row.metric_values[5].value),
                    "bounce_rate": float(row.metric_values[6].value),
                    "avg_session_duration": float(row.metric_values[7].value),
                    "avg_engagement_duration": user_engagement_duration / active_users if active_users > 0 else 0,
                    "pages_per_session": pageviews / sessions if sessions > 0 else 0,
                    "events_per_session": total_events / sessions if sessions > 0 else 0,
                    "total_events": total_events,
                    "total_conversions": conv["total_conversions"],
                    "total_revenue": conv["total_revenue"],
                })

            log.info(f"Fetched {len(daily_data)} days of daily summary from GA4 (2 requests merged)")
            return daily_data

        except Exception as e:
            log.error(f"Error fetching GA4 daily summary: {str(e)}")
            return []

    async def _fetch_device_breakdown(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch daily metrics by device category (desktop, mobile, tablet).
        """
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="deviceCategory"),
                ],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="newUsers"),
                    Metric(name="engagedSessions"),
                    Metric(name="bounceRate"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
            )

            response = self.client.run_report(request)

            devices = []
            for row in response.rows:
                devices.append({
                    "date": row.dimension_values[0].value,
                    "device_category": row.dimension_values[1].value,
                    "sessions": int(row.metric_values[0].value),
                    "active_users": int(row.metric_values[1].value),
                    "new_users": int(row.metric_values[2].value),
                    "engaged_sessions": int(row.metric_values[3].value),
                    "bounce_rate": float(row.metric_values[4].value),
                    "avg_session_duration": float(row.metric_values[5].value),
                    "conversions": int(float(row.metric_values[6].value)),
                    "total_revenue": float(row.metric_values[7].value),
                })

            log.info(f"Fetched {len(devices)} device breakdown records from GA4")
            return devices

        except Exception as e:
            log.error(f"Error fetching GA4 device breakdown: {str(e)}")
            return []

    async def _fetch_geo_breakdown(
        self,
        start_date: datetime,
        end_date: datetime,
        granularity: str = "country"
    ) -> List[Dict]:
        """
        Fetch daily metrics by geography with pagination.

        Args:
            granularity: "country" (default), "region", or "city"
        """
        dimensions = [Dimension(name="date"), Dimension(name="country")]

        if granularity in ("region", "city"):
            dimensions.append(Dimension(name="region"))
        if granularity == "city":
            dimensions.append(Dimension(name="city"))

        all_geo_data = []
        offset = 0
        limit = 10000  # GA4 max rows per request

        while True:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(
                        start_date=self._format_date(start_date),
                        end_date=self._format_date(end_date)
                    )],
                    dimensions=dimensions,
                    metrics=[
                        Metric(name="sessions"),
                        Metric(name="activeUsers"),
                        Metric(name="newUsers"),
                        Metric(name="engagedSessions"),
                        Metric(name="bounceRate"),
                        Metric(name="conversions"),
                        Metric(name="totalRevenue"),
                    ],
                    offset=offset,
                    limit=limit,
                )

                response = self.client.run_report(request)

                if not response.rows:
                    break

                for row in response.rows:
                    geo_record = {
                        "date": row.dimension_values[0].value,
                        "country": row.dimension_values[1].value,
                        "region": row.dimension_values[2].value if len(row.dimension_values) > 2 else None,
                        "city": row.dimension_values[3].value if len(row.dimension_values) > 3 else None,
                        "sessions": int(row.metric_values[0].value),
                        "active_users": int(row.metric_values[1].value),
                        "new_users": int(row.metric_values[2].value),
                        "engaged_sessions": int(row.metric_values[3].value),
                        "bounce_rate": float(row.metric_values[4].value),
                        "conversions": int(float(row.metric_values[5].value)),
                        "total_revenue": float(row.metric_values[6].value),
                    }
                    all_geo_data.append(geo_record)

                if len(response.rows) < limit:
                    break

                offset += limit
                log.info(f"Fetched {len(all_geo_data)} geo records, fetching more...")

            except Exception as e:
                log.error(f"Error fetching GA4 geo breakdown at offset {offset}: {str(e)}")
                break

        log.info(f"Fetched {len(all_geo_data)} total geo breakdown records from GA4")
        return all_geo_data

    async def _fetch_user_type_breakdown(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch daily metrics by user type (new vs returning).
        """
        try:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                date_ranges=[DateRange(
                    start_date=self._format_date(start_date),
                    end_date=self._format_date(end_date)
                )],
                dimensions=[
                    Dimension(name="date"),
                    Dimension(name="newVsReturning"),
                ],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="sessions"),
                    Metric(name="engagedSessions"),
                    Metric(name="screenPageViews"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="conversions"),
                    Metric(name="totalRevenue"),
                ],
            )

            response = self.client.run_report(request)

            user_types = []
            for row in response.rows:
                user_types.append({
                    "date": row.dimension_values[0].value,
                    "user_type": row.dimension_values[1].value,
                    "users": int(row.metric_values[0].value),
                    "sessions": int(row.metric_values[1].value),
                    "engaged_sessions": int(row.metric_values[2].value),
                    "pageviews": int(row.metric_values[3].value),
                    "avg_session_duration": float(row.metric_values[4].value),
                    "conversions": int(float(row.metric_values[5].value)),
                    "total_revenue": float(row.metric_values[6].value),
                })

            log.info(f"Fetched {len(user_types)} user type records from GA4")
            return user_types

        except Exception as e:
            log.error(f"Error fetching GA4 user type breakdown: {str(e)}")
            return []

    async def _fetch_page_performance_paginated(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch page-level performance data with pagination for large datasets.
        """
        all_pages = []
        offset = 0
        limit = 10000

        while True:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(
                        start_date=self._format_date(start_date),
                        end_date=self._format_date(end_date)
                    )],
                    metrics=[
                        Metric(name="screenPageViews"),
                        Metric(name="sessions"),
                        Metric(name="bounceRate"),
                        Metric(name="averageSessionDuration"),
                    ],
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="pagePath"),
                        Dimension(name="pageTitle"),
                    ],
                    offset=offset,
                    limit=limit,
                )

                response = self.client.run_report(request)

                if not response.rows:
                    break

                for row in response.rows:
                    pageviews = int(row.metric_values[0].value)
                    sessions = int(row.metric_values[1].value)
                    all_pages.append({
                        "date": row.dimension_values[0].value,
                        "path": row.dimension_values[1].value,
                        "title": row.dimension_values[2].value,
                        "pageviews": pageviews,
                        "sessions": sessions,
                        "bounce_rate": float(row.metric_values[2].value),
                        "avg_time_on_page": float(row.metric_values[3].value),
                    })

                if len(response.rows) < limit:
                    break

                offset += limit
                log.info(f"Fetched {len(all_pages)} page records, fetching more...")

            except Exception as e:
                log.error(f"Error fetching GA4 pages at offset {offset}: {str(e)}")
                break

        log.info(f"Fetched {len(all_pages)} total page records from GA4")
        return all_pages

    async def _fetch_traffic_sources_paginated(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch traffic source breakdown with pagination for large datasets.
        """
        all_sources = []
        offset = 0
        limit = 10000

        while True:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(
                        start_date=self._format_date(start_date),
                        end_date=self._format_date(end_date)
                    )],
                    metrics=[
                        Metric(name="sessions"),
                        Metric(name="totalUsers"),
                        Metric(name="newUsers"),
                        Metric(name="engagedSessions"),
                        Metric(name="bounceRate"),
                        Metric(name="averageSessionDuration"),
                        Metric(name="conversions"),
                        Metric(name="totalRevenue"),
                    ],
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="sessionSource"),
                        Dimension(name="sessionMedium"),
                        Dimension(name="sessionCampaignName"),
                    ],
                    offset=offset,
                    limit=limit,
                )

                response = self.client.run_report(request)

                if not response.rows:
                    break

                for row in response.rows:
                    all_sources.append({
                        "date": row.dimension_values[0].value,
                        "source": row.dimension_values[1].value,
                        "medium": row.dimension_values[2].value,
                        "campaign": row.dimension_values[3].value,
                        "sessions": int(row.metric_values[0].value),
                        "total_users": int(row.metric_values[1].value),
                        "new_users": int(row.metric_values[2].value),
                        "engaged_sessions": int(row.metric_values[3].value),
                        "bounce_rate": float(row.metric_values[4].value),
                        "avg_session_duration": float(row.metric_values[5].value),
                        "conversions": int(float(row.metric_values[6].value)),
                        "revenue": float(row.metric_values[7].value),
                    })

                if len(response.rows) < limit:
                    break

                offset += limit
                log.info(f"Fetched {len(all_sources)} traffic source records, fetching more...")

            except Exception as e:
                log.error(f"Error fetching GA4 traffic sources at offset {offset}: {str(e)}")
                break

        log.info(f"Fetched {len(all_sources)} total traffic source records from GA4")
        return all_sources

    async def _fetch_landing_pages_paginated(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch landing page performance with pagination for large datasets.
        """
        all_landing_pages = []
        offset = 0
        limit = 10000

        while True:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(
                        start_date=self._format_date(start_date),
                        end_date=self._format_date(end_date)
                    )],
                    metrics=[
                        Metric(name="sessions"),
                        Metric(name="bounceRate"),
                        Metric(name="averageSessionDuration"),
                        Metric(name="conversions"),
                        Metric(name="totalRevenue"),
                    ],
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="landingPage"),
                        Dimension(name="sessionSource"),
                        Dimension(name="sessionMedium"),
                    ],
                    offset=offset,
                    limit=limit,
                )

                response = self.client.run_report(request)

                if not response.rows:
                    break

                for row in response.rows:
                    sessions = int(row.metric_values[0].value)
                    conversions = int(float(row.metric_values[3].value))
                    all_landing_pages.append({
                        "date": row.dimension_values[0].value,
                        "landing_page": row.dimension_values[1].value,
                        "source": row.dimension_values[2].value,
                        "medium": row.dimension_values[3].value,
                        "sessions": sessions,
                        "bounce_rate": float(row.metric_values[1].value),
                        "avg_session_duration": float(row.metric_values[2].value),
                        "conversions": conversions,
                        "conversion_rate": conversions / sessions if sessions > 0 else 0,
                        "revenue": float(row.metric_values[4].value),
                    })

                if len(response.rows) < limit:
                    break

                offset += limit
                log.info(f"Fetched {len(all_landing_pages)} landing page records, fetching more...")

            except Exception as e:
                log.error(f"Error fetching GA4 landing pages at offset {offset}: {str(e)}")
                break

        log.info(f"Fetched {len(all_landing_pages)} total landing page records from GA4")
        return all_landing_pages

    async def _fetch_product_performance_paginated(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Fetch e-commerce product performance with pagination for large catalogs.
        """
        all_products = []
        offset = 0
        limit = 10000

        while True:
            try:
                request = RunReportRequest(
                    property=f"properties/{self.property_id}",
                    date_ranges=[DateRange(
                        start_date=self._format_date(start_date),
                        end_date=self._format_date(end_date)
                    )],
                    metrics=[
                        Metric(name="itemsViewed"),
                        Metric(name="itemsAddedToCart"),
                        Metric(name="itemsPurchased"),
                        Metric(name="itemRevenue"),
                    ],
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="itemId"),
                        Dimension(name="itemName"),
                        Dimension(name="itemCategory"),
                    ],
                    offset=offset,
                    limit=limit,
                )

                response = self.client.run_report(request)

                if not response.rows:
                    break

                for row in response.rows:
                    items_viewed = int(float(row.metric_values[0].value))
                    items_added = int(float(row.metric_values[1].value))
                    all_products.append({
                        "date": row.dimension_values[0].value,
                        "item_id": row.dimension_values[1].value,
                        "item_name": row.dimension_values[2].value,
                        "item_category": row.dimension_values[3].value,
                        "items_viewed": items_viewed,
                        "items_added_to_cart": items_added,
                        "items_purchased": int(float(row.metric_values[2].value)),
                        "item_revenue": float(row.metric_values[3].value),
                        "add_to_cart_rate": items_added / items_viewed if items_viewed > 0 else 0,
                    })

                if len(response.rows) < limit:
                    break

                offset += limit
                log.info(f"Fetched {len(all_products)} product records, fetching more...")

            except Exception as e:
                log.error(f"Error fetching GA4 products at offset {offset}: {str(e)}")
                break

        log.info(f"Fetched {len(all_products)} total product records from GA4")
        return all_products
