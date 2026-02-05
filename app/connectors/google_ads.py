"""
Google Ads Connector

Syncs campaign performance, ad groups, products, search terms, and click data from Google Ads API.
"""
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError:
    GoogleAdsClient = None
    GoogleAdsException = Exception

from app.connectors.base import BaseConnector
from app.models.google_ads_data import (
    GoogleAdsCampaign, GoogleAdsAdGroup, GoogleAdsProductPerformance,
    GoogleAdsSearchTerm, GoogleAdsClick
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class GoogleAdsConnector(BaseConnector):
    """
    Google Ads API connector

    Syncs:
    - Campaign performance (daily)
    - Ad group performance (daily)
    - Shopping/Product performance (daily)
    - Search terms report
    - Click data (for attribution)
    """

    def __init__(self, db: Session):
        super().__init__(db, source_name="google_ads", source_type="advertising")
        self.client = None
        self.customer_id = settings.google_ads_customer_id

    async def authenticate(self) -> bool:
        """
        Authenticate with Google Ads API

        Uses OAuth2 credentials from settings:
        - developer_token
        - client_id
        - client_secret
        - refresh_token
        - customer_id

        Returns:
            True if authentication successful
        """
        try:
            if not GoogleAdsClient:
                log.error("Google Ads SDK not installed. Install with: pip install google-ads")
                return False

            # Check required credentials
            if not all([
                settings.google_ads_developer_token,
                settings.google_ads_client_id,
                settings.google_ads_client_secret,
                settings.google_ads_refresh_token,
                settings.google_ads_customer_id
            ]):
                log.error("Missing Google Ads credentials in settings")
                return False

            # Build credentials dict
            credentials = {
                "developer_token": settings.google_ads_developer_token,
                "client_id": settings.google_ads_client_id,
                "client_secret": settings.google_ads_client_secret,
                "refresh_token": settings.google_ads_refresh_token,
                "use_proto_plus": True
            }

            # Initialize client
            self.client = GoogleAdsClient.load_from_dict(credentials)

            # Test authentication with simple query
            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT customer.id
                FROM customer
                LIMIT 1
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)
            list(response)  # Consume generator to test connection

            self._authenticated = True
            log.info("Google Ads authentication successful")
            return True

        except Exception as e:
            log.error(f"Google Ads authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync data from Google Ads

        Args:
            start_date: Start date for sync (defaults to last sync or 2 years ago)
            end_date: End date for sync (defaults to yesterday)

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
                # Google Ads has same-day delay, use yesterday
                end_date = datetime.now() - timedelta(days=1)

            if not start_date:
                # Get last successful sync
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 2 years of data
                    start_date = datetime.now() - timedelta(days=730)

            log.info(f"Syncing Google Ads data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            # Sync campaigns
            campaigns_synced = await self._sync_campaigns(start_date, end_date)
            total_records += campaigns_synced

            # Sync ad groups
            ad_groups_synced = await self._sync_ad_groups(start_date, end_date)
            total_records += ad_groups_synced

            # Sync products (Shopping campaigns)
            products_synced = await self._sync_products(start_date, end_date)
            total_records += products_synced

            # Sync search terms
            search_terms_synced = await self._sync_search_terms(start_date, end_date)
            total_records += search_terms_synced

            # Sync clicks (for attribution)
            clicks_synced = await self._sync_clicks(start_date, end_date)
            total_records += clicks_synced

            # Calculate sync duration
            sync_duration = time.time() - sync_start_time

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(f"Google Ads sync completed: {total_records} records in {sync_duration:.1f}s")

            return {
                "success": True,
                "records_synced": total_records,
                "campaigns": campaigns_synced,
                "ad_groups": ad_groups_synced,
                "products": products_synced,
                "search_terms": search_terms_synced,
                "clicks": clicks_synced,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"Google Ads sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_campaigns(self, start_date: datetime, end_date: datetime) -> int:
        """Sync campaign performance data"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            # Build query
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.advertising_channel_type,
                    campaign.status,
                    segments.date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value,
                    metrics.ctr,
                    metrics.average_cpc,
                    metrics.search_impression_share,
                    metrics.search_budget_lost_impression_share,
                    metrics.search_rank_lost_impression_share
                FROM campaign
                WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            """

            # Execute query
            response = ga_service.search(customer_id=self.customer_id, query=query)

            records_synced = 0

            for row in response:
                campaign = row.campaign
                metrics = row.metrics
                segments = row.segments

                # Create or update record
                record = GoogleAdsCampaign(
                    campaign_id=str(campaign.id),
                    campaign_name=campaign.name,
                    campaign_type=campaign.advertising_channel_type.name,
                    campaign_status=campaign.status.name,
                    date=datetime.strptime(segments.date, '%Y-%m-%d').date(),
                    impressions=metrics.impressions,
                    clicks=metrics.clicks,
                    cost_micros=metrics.cost_micros,
                    conversions=metrics.conversions,
                    conversions_value=metrics.conversions_value,
                    ctr=metrics.ctr,
                    avg_cpc=metrics.average_cpc,
                    conversion_rate=metrics.conversions / metrics.clicks if metrics.clicks > 0 else 0,
                    search_impression_share=metrics.search_impression_share if hasattr(metrics, 'search_impression_share') else None,
                    search_budget_lost_impression_share=metrics.search_budget_lost_impression_share if hasattr(metrics, 'search_budget_lost_impression_share') else None,
                    search_rank_lost_impression_share=metrics.search_rank_lost_impression_share if hasattr(metrics, 'search_rank_lost_impression_share') else None
                )

                self.db.merge(record)
                records_synced += 1

                # Commit every 100 records
                if records_synced % 100 == 0:
                    self.db.commit()

            # Final commit
            self.db.commit()

            log.info(f"Synced {records_synced} Google Ads campaigns")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Google Ads campaigns: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_ad_groups(self, start_date: datetime, end_date: datetime) -> int:
        """Sync ad group performance data"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    ad_group.id,
                    ad_group.name,
                    ad_group.campaign,
                    ad_group.status,
                    segments.date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM ad_group
                WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            records_synced = 0

            for row in response:
                ad_group = row.ad_group
                metrics = row.metrics
                segments = row.segments

                # Extract campaign ID from resource name
                campaign_id = ad_group.campaign.split('/')[-1]

                record = GoogleAdsAdGroup(
                    ad_group_id=str(ad_group.id),
                    ad_group_name=ad_group.name,
                    campaign_id=campaign_id,
                    ad_group_status=ad_group.status.name,
                    date=datetime.strptime(segments.date, '%Y-%m-%d').date(),
                    impressions=metrics.impressions,
                    clicks=metrics.clicks,
                    cost_micros=metrics.cost_micros,
                    conversions=metrics.conversions,
                    conversions_value=metrics.conversions_value
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} Google Ads ad groups")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Google Ads ad groups: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_products(self, start_date: datetime, end_date: datetime) -> int:
        """Sync Shopping product performance"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    shopping_performance_view.resource_name,
                    segments.product_item_id,
                    segments.date,
                    campaign.id,
                    ad_group.id,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM shopping_performance_view
                WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            records_synced = 0

            for row in response:
                segments = row.segments
                metrics = row.metrics
                campaign = row.campaign
                ad_group = row.ad_group

                record = GoogleAdsProductPerformance(
                    product_item_id=segments.product_item_id,
                    campaign_id=str(campaign.id),
                    ad_group_id=str(ad_group.id) if ad_group else None,
                    date=datetime.strptime(segments.date, '%Y-%m-%d').date(),
                    impressions=metrics.impressions,
                    clicks=metrics.clicks,
                    cost_micros=metrics.cost_micros,
                    conversions=metrics.conversions,
                    conversions_value=metrics.conversions_value
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} Google Ads products")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Google Ads products: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_search_terms(self, start_date: datetime, end_date: datetime) -> int:
        """Sync search terms report"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    search_term_view.search_term,
                    campaign.id,
                    ad_group.id,
                    search_term_view.keyword.match_type,
                    segments.date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value
                FROM search_term_view
                WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            records_synced = 0

            for row in response:
                search_term_view = row.search_term_view
                campaign = row.campaign
                ad_group = row.ad_group
                segments = row.segments
                metrics = row.metrics

                record = GoogleAdsSearchTerm(
                    search_term=search_term_view.search_term,
                    campaign_id=str(campaign.id),
                    ad_group_id=str(ad_group.id),
                    keyword_match_type=search_term_view.keyword.match_type.name if search_term_view.keyword else None,
                    date=datetime.strptime(segments.date, '%Y-%m-%d').date(),
                    impressions=metrics.impressions,
                    clicks=metrics.clicks,
                    cost_micros=metrics.cost_micros,
                    conversions=metrics.conversions,
                    conversions_value=metrics.conversions_value
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} Google Ads search terms")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Google Ads search terms: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_clicks(self, start_date: datetime, end_date: datetime) -> int:
        """Sync click data (for attribution tracking)"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    click_view.gclid,
                    click_view.ad_group_ad,
                    campaign.id,
                    ad_group.id,
                    click_view.keyword_info.text,
                    segments.click_type,
                    segments.device,
                    segments.ad_network_type,
                    segments.date
                FROM click_view
                WHERE segments.date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            records_synced = 0

            for row in response:
                click_view = row.click_view
                campaign = row.campaign
                ad_group = row.ad_group
                segments = row.segments

                # Parse click date
                click_date = datetime.strptime(segments.date, '%Y-%m-%d')

                record = GoogleAdsClick(
                    gclid=click_view.gclid,
                    click_date=click_date,
                    campaign_id=str(campaign.id),
                    ad_group_id=str(ad_group.id) if ad_group else None,
                    device=segments.device.name if hasattr(segments, 'device') else None,
                    ad_network_type=segments.ad_network_type.name if hasattr(segments, 'ad_network_type') else None
                )

                self.db.merge(record)
                records_synced += 1

                if records_synced % 100 == 0:
                    self.db.commit()

            self.db.commit()

            log.info(f"Synced {records_synced} Google Ads clicks")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Google Ads clicks: {str(e)}")
            self.db.rollback()
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent campaign data"""
        try:
            latest = self.db.query(GoogleAdsCampaign).order_by(
                desc(GoogleAdsCampaign.date)
            ).first()

            if latest:
                # Convert date to datetime
                return datetime.combine(latest.date, datetime.min.time())

            return None

        except Exception as e:
            log.error(f"Error getting latest Google Ads timestamp: {str(e)}")
            return None
