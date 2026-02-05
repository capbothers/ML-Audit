"""
Google Ads data connector
Fetches campaign data, ad performance, and monitors disapproved ads
"""
from typing import Any, Dict, List
from datetime import datetime
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class GoogleAdsConnector(BaseConnector):
    """Connector for Google Ads platform"""

    def __init__(self):
        super().__init__("Google Ads")
        self.client = None
        self.customer_id = settings.google_ads_customer_id.replace("-", "")

    async def connect(self) -> bool:
        """Establish connection to Google Ads"""
        try:
            credentials = {
                "developer_token": settings.google_ads_developer_token,
                "client_id": settings.google_ads_client_id,
                "client_secret": settings.google_ads_client_secret,
                "refresh_token": settings.google_ads_refresh_token,
                "use_proto_plus": True,
            }

            if settings.google_ads_login_customer_id:
                credentials["login_customer_id"] = settings.google_ads_login_customer_id.replace("-", "")

            self.client = GoogleAdsClient.load_from_dict(credentials)
            log.info("Connected to Google Ads API")
            return True

        except Exception as e:
            log.error(f"Failed to connect to Google Ads: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate Google Ads connection"""
        try:
            if not self.client:
                await self.connect()

            ga_service = self.client.get_service("GoogleAdsService")
            query = """
                SELECT customer.id
                FROM customer
                LIMIT 1
            """
            ga_service.search(customer_id=self.customer_id, query=query)
            return True

        except Exception as e:
            log.error(f"Google Ads connection validation failed: {str(e)}")
            return False

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive Google Ads data"""
        if not self.client:
            await self.connect()

        data = {
            "campaigns": await self._fetch_campaigns(start_date, end_date),
            "ad_groups": await self._fetch_ad_groups(start_date, end_date),
            "ads": await self._fetch_ads(),
            "disapproved_ads": await self._fetch_disapproved_ads(),  # Critical for monitoring
            "keywords": await self._fetch_keywords(start_date, end_date),
            "search_terms": await self._fetch_search_terms(start_date, end_date),
            "account_info": await self._fetch_account_info(),
        }
        return data

    def _format_date(self, date: datetime) -> str:
        """Format datetime to Google Ads date string"""
        return date.strftime("%Y-%m-%d")

    async def _fetch_campaigns(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch campaign performance data"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    campaign.advertising_channel_type,
                    campaign.bidding_strategy_type,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.conversions_value,
                    metrics.average_cpc,
                    metrics.ctr
                FROM campaign
                WHERE segments.date >= '{self._format_date(start_date)}'
                    AND segments.date <= '{self._format_date(end_date)}'
                ORDER BY metrics.cost_micros DESC
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            campaigns = []
            for row in response:
                campaigns.append({
                    "id": row.campaign.id,
                    "name": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "bidding_strategy": row.campaign.bidding_strategy_type.name,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "conversion_value": row.metrics.conversions_value,
                    "avg_cpc": row.metrics.average_cpc / 1_000_000 if row.metrics.average_cpc else 0,
                    "ctr": row.metrics.ctr,
                })

            log.info(f"Fetched {len(campaigns)} campaigns from Google Ads")
            return campaigns

        except GoogleAdsException as e:
            log.error(f"Google Ads API error fetching campaigns: {e}")
            return []
        except Exception as e:
            log.error(f"Error fetching Google Ads campaigns: {str(e)}")
            return []

    async def _fetch_ad_groups(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch ad group performance data"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    ad_group.id,
                    ad_group.name,
                    ad_group.status,
                    campaign.name,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.ctr
                FROM ad_group
                WHERE segments.date >= '{self._format_date(start_date)}'
                    AND segments.date <= '{self._format_date(end_date)}'
                ORDER BY metrics.cost_micros DESC
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            ad_groups = []
            for row in response:
                ad_groups.append({
                    "id": row.ad_group.id,
                    "name": row.ad_group.name,
                    "status": row.ad_group.status.name,
                    "campaign": row.campaign.name,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "ctr": row.metrics.ctr,
                })

            log.info(f"Fetched {len(ad_groups)} ad groups from Google Ads")
            return ad_groups

        except Exception as e:
            log.error(f"Error fetching Google Ads ad groups: {str(e)}")
            return []

    async def _fetch_ads(self) -> List[Dict]:
        """Fetch all ads"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.name,
                    ad_group_ad.status,
                    ad_group_ad.policy_summary.approval_status,
                    ad_group_ad.ad.type,
                    ad_group.name,
                    campaign.name
                FROM ad_group_ad
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            ads = []
            for row in response:
                ads.append({
                    "id": row.ad_group_ad.ad.id,
                    "name": row.ad_group_ad.ad.name,
                    "status": row.ad_group_ad.status.name,
                    "approval_status": row.ad_group_ad.policy_summary.approval_status.name,
                    "type": row.ad_group_ad.ad.type_.name,
                    "ad_group": row.ad_group.name,
                    "campaign": row.campaign.name,
                })

            log.info(f"Fetched {len(ads)} ads from Google Ads")
            return ads

        except Exception as e:
            log.error(f"Error fetching Google Ads ads: {str(e)}")
            return []

    async def _fetch_disapproved_ads(self) -> List[Dict]:
        """
        Fetch disapproved ads - CRITICAL for monitoring ad account health
        This helps identify policy violations before they impact campaigns
        """
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                    ad_group_ad.ad.id,
                    ad_group_ad.ad.name,
                    ad_group_ad.status,
                    ad_group_ad.policy_summary.approval_status,
                    ad_group_ad.policy_summary.policy_topic_entries,
                    ad_group_ad.policy_summary.review_status,
                    ad_group.name,
                    campaign.name
                FROM ad_group_ad
                WHERE ad_group_ad.policy_summary.approval_status = 'DISAPPROVED'
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            disapproved_ads = []
            for row in response:
                # Extract policy violations
                violations = []
                if row.ad_group_ad.policy_summary.policy_topic_entries:
                    for entry in row.ad_group_ad.policy_summary.policy_topic_entries:
                        violations.append({
                            "topic": entry.topic,
                            "type": entry.type_.name if entry.type_ else "UNKNOWN",
                        })

                disapproved_ads.append({
                    "id": row.ad_group_ad.ad.id,
                    "name": row.ad_group_ad.ad.name,
                    "status": row.ad_group_ad.status.name,
                    "approval_status": row.ad_group_ad.policy_summary.approval_status.name,
                    "review_status": row.ad_group_ad.policy_summary.review_status.name,
                    "ad_group": row.ad_group.name,
                    "campaign": row.campaign.name,
                    "violations": violations,
                })

            log.warning(f"Found {len(disapproved_ads)} disapproved ads in Google Ads")
            return disapproved_ads

        except Exception as e:
            log.error(f"Error fetching disapproved ads: {str(e)}")
            return []

    async def _fetch_keywords(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch keyword performance data"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    ad_group_criterion.criterion_id,
                    ad_group_criterion.keyword.text,
                    ad_group_criterion.keyword.match_type,
                    ad_group_criterion.status,
                    ad_group.name,
                    campaign.name,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.ctr,
                    metrics.average_cpc
                FROM keyword_view
                WHERE segments.date >= '{self._format_date(start_date)}'
                    AND segments.date <= '{self._format_date(end_date)}'
                ORDER BY metrics.cost_micros DESC
                LIMIT 500
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            keywords = []
            for row in response:
                keywords.append({
                    "id": row.ad_group_criterion.criterion_id,
                    "keyword": row.ad_group_criterion.keyword.text,
                    "match_type": row.ad_group_criterion.keyword.match_type.name,
                    "status": row.ad_group_criterion.status.name,
                    "ad_group": row.ad_group.name,
                    "campaign": row.campaign.name,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "ctr": row.metrics.ctr,
                    "avg_cpc": row.metrics.average_cpc / 1_000_000 if row.metrics.average_cpc else 0,
                })

            log.info(f"Fetched {len(keywords)} keywords from Google Ads")
            return keywords

        except Exception as e:
            log.error(f"Error fetching Google Ads keywords: {str(e)}")
            return []

    async def _fetch_search_terms(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch search terms report - critical for finding negative keywords"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = f"""
                SELECT
                    search_term_view.search_term,
                    campaign.name,
                    ad_group.name,
                    metrics.cost_micros,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.conversions,
                    metrics.ctr
                FROM search_term_view
                WHERE segments.date >= '{self._format_date(start_date)}'
                    AND segments.date <= '{self._format_date(end_date)}'
                ORDER BY metrics.cost_micros DESC
                LIMIT 500
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            search_terms = []
            for row in response:
                search_terms.append({
                    "search_term": row.search_term_view.search_term,
                    "campaign": row.campaign.name,
                    "ad_group": row.ad_group.name,
                    "cost": row.metrics.cost_micros / 1_000_000,
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": row.metrics.conversions,
                    "ctr": row.metrics.ctr,
                })

            log.info(f"Fetched {len(search_terms)} search terms from Google Ads")
            return search_terms

        except Exception as e:
            log.error(f"Error fetching search terms: {str(e)}")
            return []

    async def _fetch_account_info(self) -> Dict:
        """Fetch Google Ads account information"""
        try:
            ga_service = self.client.get_service("GoogleAdsService")

            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code,
                    customer.time_zone,
                    customer.status
                FROM customer
                LIMIT 1
            """

            response = ga_service.search(customer_id=self.customer_id, query=query)

            for row in response:
                return {
                    "id": row.customer.id,
                    "name": row.customer.descriptive_name,
                    "currency": row.customer.currency_code,
                    "timezone": row.customer.time_zone,
                    "status": row.customer.status.name,
                }

            return {}

        except Exception as e:
            log.error(f"Error fetching account info: {str(e)}")
            return {}
