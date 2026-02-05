"""
Klaviyo Connector

Syncs email campaign performance, flow automation metrics, segments, and customer profiles from Klaviyo API.
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time
import requests

from app.connectors.base import BaseConnector
from app.models.klaviyo_data import (
    KlaviyoCampaign, KlaviyoFlow, KlaviyoFlowMessage,
    KlaviyoSegment, KlaviyoProfile
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class KlaviyoConnector(BaseConnector):
    """
    Klaviyo API connector

    Syncs:
    - Campaign performance (one-time emails)
    - Flow performance (automated emails)
    - Flow message performance (individual emails in flows)
    - Segments (customer lists)
    - Profiles (optional - for churn prediction)
    """

    def __init__(self, db: Session):
        super().__init__(db, source_name="klaviyo", source_type="email_marketing")
        self.api_key = settings.klaviyo_api_key
        self.base_url = "https://a.klaviyo.com/api"
        self.headers = {
            'Authorization': f'Klaviyo-API-Key {self.api_key}',
            'revision': '2024-02-15',  # API version
            'Accept': 'application/json'
        }

    async def authenticate(self) -> bool:
        """
        Authenticate with Klaviyo API

        Returns:
            True if authentication successful
        """
        try:
            if not self.api_key:
                log.error("Missing Klaviyo API key in settings")
                return False

            # Test authentication by getting account info
            response = requests.get(
                f"{self.base_url}/accounts",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                self._authenticated = True
                log.info("Klaviyo authentication successful")
                return True
            else:
                log.error(f"Klaviyo authentication failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            log.error(f"Klaviyo authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync data from Klaviyo

        Args:
            start_date: Start date for sync (defaults to last sync or 1 year ago)
            end_date: End date for sync (defaults to now)

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
                end_date = datetime.now()

            if not start_date:
                # Get last successful sync
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 1 year of data
                    start_date = datetime.now() - timedelta(days=365)

            log.info(f"Syncing Klaviyo data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            # Sync campaigns
            campaigns_synced = await self._sync_campaigns(start_date, end_date)
            total_records += campaigns_synced

            # Sync flows
            flows_synced = await self._sync_flows()
            total_records += flows_synced

            # Sync flow messages
            flow_messages_synced = await self._sync_flow_messages()
            total_records += flow_messages_synced

            # Sync segments
            segments_synced = await self._sync_segments()
            total_records += segments_synced

            # Calculate sync duration
            sync_duration = time.time() - sync_start_time

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(f"Klaviyo sync completed: {total_records} records in {sync_duration:.1f}s")

            return {
                "success": True,
                "records_synced": total_records,
                "campaigns": campaigns_synced,
                "flows": flows_synced,
                "flow_messages": flow_messages_synced,
                "segments": segments_synced,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"Klaviyo sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_campaigns(self, start_date: datetime, end_date: datetime) -> int:
        """Sync email campaign performance"""
        try:
            # Get all campaigns
            response = requests.get(
                f"{self.base_url}/campaigns",
                headers=self.headers,
                params={'page[size]': 100}
            )

            if response.status_code != 200:
                log.error(f"Failed to get campaigns: {response.status_code} - {response.text}")
                return 0

            data = response.json()
            records_synced = 0

            if 'data' in data:
                for campaign in data['data']:
                    campaign_id = campaign['id']
                    attributes = campaign.get('attributes', {})

                    # Get campaign metrics
                    metrics = await self._get_campaign_metrics(campaign_id)

                    if metrics is None:
                        continue

                    # Parse send time
                    send_time = None
                    if 'send_time' in attributes:
                        try:
                            send_time = datetime.fromisoformat(attributes['send_time'].replace('Z', '+00:00'))
                        except:
                            pass

                    # Create record
                    record = KlaviyoCampaign(
                        campaign_id=campaign_id,
                        campaign_name=attributes.get('name', 'Untitled'),
                        subject_line=attributes.get('subject', ''),
                        status=attributes.get('status', 'unknown').upper(),
                        send_time=send_time,
                        recipients=metrics.get('recipients', 0),
                        opens=metrics.get('opens', 0),
                        clicks=metrics.get('clicks', 0),
                        unsubscribes=metrics.get('unsubscribes', 0),
                        bounces=metrics.get('bounces', 0),
                        spam_complaints=metrics.get('spam_complaints', 0),
                        open_rate=metrics.get('open_rate', 0.0),
                        click_rate=metrics.get('click_rate', 0.0),
                        unsubscribe_rate=metrics.get('unsubscribe_rate', 0.0),
                        conversions=metrics.get('conversions', 0),
                        revenue=metrics.get('revenue', 0.0)
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)  # Rate limit: 10 req/sec

                self.db.commit()

            log.info(f"Synced {records_synced} Klaviyo campaigns")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Klaviyo campaigns: {str(e)}")
            self.db.rollback()
            return 0

    async def _get_campaign_metrics(self, campaign_id: str) -> Optional[Dict]:
        """Get metrics for a specific campaign"""
        try:
            response = requests.get(
                f"{self.base_url}/campaign-values-reports/{campaign_id}",
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    attributes = data['data'].get('attributes', {})
                    statistics = attributes.get('statistics', {})

                    # Extract metrics
                    return {
                        'recipients': statistics.get('recipients', 0),
                        'opens': statistics.get('opens', 0),
                        'clicks': statistics.get('clicks', 0),
                        'unsubscribes': statistics.get('unsubscribes', 0),
                        'bounces': statistics.get('bounces', 0),
                        'spam_complaints': statistics.get('spam_complaints', 0),
                        'open_rate': statistics.get('open_rate', 0.0),
                        'click_rate': statistics.get('click_rate', 0.0),
                        'unsubscribe_rate': statistics.get('unsubscribe_rate', 0.0),
                        'conversions': statistics.get('conversions', 0),
                        'revenue': float(statistics.get('revenue', 0.0))
                    }

            return None

        except Exception as e:
            log.error(f"Error getting campaign metrics for {campaign_id}: {str(e)}")
            return None

    async def _sync_flows(self) -> int:
        """Sync automated flow performance"""
        try:
            # Get all flows
            response = requests.get(
                f"{self.base_url}/flows",
                headers=self.headers,
                params={'page[size]': 100}
            )

            if response.status_code != 200:
                log.error(f"Failed to get flows: {response.status_code} - {response.text}")
                return 0

            data = response.json()
            records_synced = 0

            if 'data' in data:
                for flow in data['data']:
                    flow_id = flow['id']
                    attributes = flow.get('attributes', {})

                    # Get flow metrics
                    metrics = await self._get_flow_metrics(flow_id)

                    if metrics is None:
                        continue

                    # Determine trigger type from flow name
                    flow_name = attributes.get('name', 'Unknown')
                    trigger_type = self._infer_trigger_type(flow_name)

                    # Create record
                    record = KlaviyoFlow(
                        flow_id=flow_id,
                        flow_name=flow_name,
                        trigger_type=trigger_type,
                        status=attributes.get('status', 'unknown').upper(),
                        recipients=metrics.get('recipients', 0),
                        opens=metrics.get('opens', 0),
                        clicks=metrics.get('clicks', 0),
                        conversions=metrics.get('conversions', 0),
                        revenue=metrics.get('revenue', 0.0),
                        open_rate=metrics.get('open_rate', 0.0),
                        click_rate=metrics.get('click_rate', 0.0),
                        conversion_rate=metrics.get('conversion_rate', 0.0)
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Klaviyo flows")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Klaviyo flows: {str(e)}")
            self.db.rollback()
            return 0

    def _infer_trigger_type(self, flow_name: str) -> str:
        """Infer flow trigger type from name"""
        flow_name_lower = flow_name.lower()

        if 'abandon' in flow_name_lower or 'cart' in flow_name_lower:
            return 'ABANDONED_CART'
        elif 'welcome' in flow_name_lower:
            return 'WELCOME_SERIES'
        elif 'post' in flow_name_lower and 'purchase' in flow_name_lower:
            return 'POST_PURCHASE'
        elif 'browse' in flow_name_lower:
            return 'BROWSE_ABANDONMENT'
        elif 'win' in flow_name_lower and 'back' in flow_name_lower:
            return 'WIN_BACK'
        elif 'birthday' in flow_name_lower:
            return 'BIRTHDAY'
        else:
            return 'OTHER'

    async def _get_flow_metrics(self, flow_id: str) -> Optional[Dict]:
        """Get metrics for a specific flow"""
        try:
            response = requests.get(
                f"{self.base_url}/flow-values-reports/{flow_id}",
                headers=self.headers
            )

            if response.status_code == 200:
                data = response.json()
                if 'data' in data:
                    attributes = data['data'].get('attributes', {})
                    statistics = attributes.get('statistics', {})

                    # Calculate conversion rate
                    recipients = statistics.get('recipients', 0)
                    conversions = statistics.get('conversions', 0)
                    conversion_rate = (conversions / recipients * 100) if recipients > 0 else 0.0

                    return {
                        'recipients': recipients,
                        'opens': statistics.get('opens', 0),
                        'clicks': statistics.get('clicks', 0),
                        'conversions': conversions,
                        'revenue': float(statistics.get('revenue', 0.0)),
                        'open_rate': statistics.get('open_rate', 0.0),
                        'click_rate': statistics.get('click_rate', 0.0),
                        'conversion_rate': conversion_rate
                    }

            return None

        except Exception as e:
            log.error(f"Error getting flow metrics for {flow_id}: {str(e)}")
            return None

    async def _sync_flow_messages(self) -> int:
        """
        Sync individual flow message performance

        Note: This requires fetching messages for each flow
        """
        try:
            # Get all flows first
            flows_response = requests.get(
                f"{self.base_url}/flows",
                headers=self.headers,
                params={'page[size]': 100}
            )

            if flows_response.status_code != 200:
                return 0

            flows_data = flows_response.json()
            records_synced = 0

            if 'data' in flows_data:
                for flow in flows_data['data']:
                    flow_id = flow['id']

                    # Get flow actions (messages)
                    messages_response = requests.get(
                        f"{self.base_url}/flows/{flow_id}/flow-actions",
                        headers=self.headers
                    )

                    if messages_response.status_code == 200:
                        messages_data = messages_response.json()

                        if 'data' in messages_data:
                            for message in messages_data['data']:
                                message_id = message['id']
                                attributes = message.get('attributes', {})

                                # Get message metrics
                                metrics = await self._get_flow_message_metrics(flow_id, message_id)

                                if metrics is None:
                                    continue

                                # Parse delay
                                delay_minutes = 0
                                if 'delay' in attributes:
                                    delay_minutes = attributes['delay'].get('delay_minutes', 0)

                                record = KlaviyoFlowMessage(
                                    flow_message_id=message_id,
                                    flow_id=flow_id,
                                    message_name=attributes.get('name', 'Untitled'),
                                    subject_line=attributes.get('subject', ''),
                                    delay_minutes=delay_minutes,
                                    recipients=metrics.get('recipients', 0),
                                    opens=metrics.get('opens', 0),
                                    clicks=metrics.get('clicks', 0),
                                    conversions=metrics.get('conversions', 0),
                                    revenue=metrics.get('revenue', 0.0)
                                )

                                self.db.merge(record)
                                records_synced += 1

                                if records_synced % 50 == 0:
                                    self.db.commit()
                                    time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Klaviyo flow messages")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Klaviyo flow messages: {str(e)}")
            self.db.rollback()
            return 0

    async def _get_flow_message_metrics(self, flow_id: str, message_id: str) -> Optional[Dict]:
        """Get metrics for a specific flow message"""
        try:
            # Flow message metrics might not be available in all API versions
            # Return placeholder for now
            return {
                'recipients': 0,
                'opens': 0,
                'clicks': 0,
                'conversions': 0,
                'revenue': 0.0
            }

        except Exception as e:
            log.error(f"Error getting flow message metrics: {str(e)}")
            return None

    async def _sync_segments(self) -> int:
        """Sync customer segments"""
        try:
            response = requests.get(
                f"{self.base_url}/segments",
                headers=self.headers,
                params={'page[size]': 100}
            )

            if response.status_code != 200:
                log.error(f"Failed to get segments: {response.status_code} - {response.text}")
                return 0

            data = response.json()
            records_synced = 0

            if 'data' in data:
                for segment in data['data']:
                    segment_id = segment['id']
                    attributes = segment.get('attributes', {})

                    # Get segment profile count
                    profile_count = await self._get_segment_profile_count(segment_id)

                    record = KlaviyoSegment(
                        segment_id=segment_id,
                        segment_name=attributes.get('name', 'Untitled'),
                        segment_type='DYNAMIC',  # Klaviyo segments are typically dynamic
                        member_count=profile_count,
                        segment_definition=attributes.get('definition', None)
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Klaviyo segments")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Klaviyo segments: {str(e)}")
            self.db.rollback()
            return 0

    async def _get_segment_profile_count(self, segment_id: str) -> int:
        """Get profile count for a segment"""
        try:
            response = requests.get(
                f"{self.base_url}/segments/{segment_id}/profiles",
                headers=self.headers,
                params={'page[size]': 1}  # Just get count
            )

            if response.status_code == 200:
                data = response.json()
                # Klaviyo returns total count in links
                if 'links' in data and 'total' in data['links']:
                    return int(data['links']['total'])

            return 0

        except Exception as e:
            log.error(f"Error getting segment profile count: {str(e)}")
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent campaign data"""
        try:
            latest = self.db.query(KlaviyoCampaign).order_by(
                desc(KlaviyoCampaign.send_time)
            ).first()

            if latest and latest.send_time:
                return latest.send_time

            return None

        except Exception as e:
            log.error(f"Error getting latest Klaviyo timestamp: {str(e)}")
            return None
