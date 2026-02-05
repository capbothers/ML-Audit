"""
Klaviyo data connector
Fetches email campaign data, metrics, and customer engagement
"""
from typing import Any, Dict, List
from datetime import datetime
import aiohttp
from app.connectors.base_connector import BaseConnector
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class KlaviyoConnector(BaseConnector):
    """Connector for Klaviyo email marketing platform"""

    def __init__(self):
        super().__init__("Klaviyo")
        self.api_key = settings.klaviyo_api_key
        self.base_url = "https://a.klaviyo.com/api"
        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.api_key}",
            "revision": "2024-02-15",
            "Accept": "application/json"
        }

    async def connect(self) -> bool:
        """Test Klaviyo API connection"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/accounts/",
                    headers=self.headers
                ) as response:
                    if response.status == 200:
                        log.info("Connected to Klaviyo API")
                        return True
                    return False
        except Exception as e:
            log.error(f"Failed to connect to Klaviyo: {str(e)}")
            return False

    async def validate_connection(self) -> bool:
        """Validate Klaviyo connection"""
        return await self.connect()

    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch comprehensive Klaviyo data"""
        flows = await self._fetch_flows()
        flow_messages = await self._fetch_flow_messages(flows)

        data = {
            "campaigns": await self._fetch_campaigns(start_date, end_date),
            "flows": flows,
            "flow_messages": flow_messages,
            "metrics": await self._fetch_metrics(start_date, end_date),
            "lists": await self._fetch_lists(),
            "segments": await self._fetch_segments()
        }
        return data

    async def _fetch_campaigns(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch email campaign data"""
        try:
            async with aiohttp.ClientSession() as session:
                campaigns = []
                url = f"{self.base_url}/campaigns/"

                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()

                        for campaign in data.get("data", []):
                            campaign_id = campaign["id"]

                            # Fetch campaign metrics
                            metrics = await self._fetch_campaign_metrics(session, campaign_id)

                            campaigns.append({
                                "id": campaign_id,
                                "name": campaign["attributes"].get("name"),
                                "subject": campaign["attributes"].get("subject_line"),
                                "status": campaign["attributes"].get("status"),
                                "send_time": campaign["attributes"].get("send_time"),
                                "created_at": campaign["attributes"].get("created_at"),
                                "updated_at": campaign["attributes"].get("updated_at"),
                                "metrics": metrics
                            })

                log.info(f"Fetched {len(campaigns)} campaigns from Klaviyo")
                return campaigns

        except Exception as e:
            log.error(f"Error fetching Klaviyo campaigns: {str(e)}")
            return []

    async def _fetch_campaign_metrics(self, session: aiohttp.ClientSession, campaign_id: str) -> Dict:
        """Fetch metrics for a specific campaign"""
        try:
            url = f"{self.base_url}/campaign-metrics/{campaign_id}/"
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    attrs = data.get("data", {}).get("attributes", {})
                    return {
                        "sent": attrs.get("sent", 0),
                        "delivered": attrs.get("delivered", 0),
                        "opens": attrs.get("opens", 0),
                        "unique_opens": attrs.get("unique_opens", 0),
                        "clicks": attrs.get("clicks", 0),
                        "unique_clicks": attrs.get("unique_clicks", 0),
                        "bounces": attrs.get("bounces", 0),
                        "unsubscribes": attrs.get("unsubscribes", 0),
                        "spam_complaints": attrs.get("spam_complaints", 0),
                        "open_rate": attrs.get("open_rate", 0),
                        "click_rate": attrs.get("click_rate", 0),
                    }
                return {}
        except Exception as e:
            log.error(f"Error fetching campaign metrics: {str(e)}")
            return {}

    async def _fetch_flows(self) -> List[Dict]:
        """Fetch automated flow data"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/flows/"
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        flows = []

                        for flow in data.get("data", []):
                            flows.append({
                                "id": flow["id"],
                                "name": flow["attributes"].get("name"),
                                "status": flow["attributes"].get("status"),
                                "created_at": flow["attributes"].get("created"),
                                "updated_at": flow["attributes"].get("updated"),
                            })

                        log.info(f"Fetched {len(flows)} flows from Klaviyo")
                        return flows

        except Exception as e:
            log.error(f"Error fetching Klaviyo flows: {str(e)}")
            return []

    async def _fetch_flow_messages(self, flows: List[Dict]) -> List[Dict]:
        """Fetch flow messages (actions) with their metrics for each flow"""
        all_messages = []

        try:
            async with aiohttp.ClientSession() as session:
                for flow in flows:
                    flow_id = flow.get("id")
                    if not flow_id:
                        continue

                    # Fetch flow actions (messages) for this flow
                    url = f"{self.base_url}/flows/{flow_id}/flow-actions/"

                    try:
                        async with session.get(url, headers=self.headers) as response:
                            if response.status == 200:
                                data = await response.json()

                                for action in data.get("data", []):
                                    action_id = action.get("id")
                                    attrs = action.get("attributes", {})

                                    # Get message metrics
                                    metrics = await self._fetch_flow_message_metrics(session, action_id)

                                    # Get message content (for subject line)
                                    message_content = await self._fetch_flow_message_content(session, action_id)

                                    all_messages.append({
                                        "message_id": action_id,
                                        "flow_id": flow_id,
                                        "message_name": attrs.get("name"),
                                        "action_type": attrs.get("action_type"),
                                        "status": attrs.get("status"),
                                        "subject_line": message_content.get("subject"),
                                        "created_at": attrs.get("created"),
                                        "updated_at": attrs.get("updated"),
                                        "metrics": metrics
                                    })

                            elif response.status == 404:
                                # Flow might not have actions
                                continue
                            else:
                                log.warning(f"Failed to fetch flow actions for {flow_id}: {response.status}")

                    except Exception as e:
                        log.warning(f"Error fetching flow actions for {flow_id}: {e}")
                        continue

                log.info(f"Fetched {len(all_messages)} flow messages from Klaviyo")
                return all_messages

        except Exception as e:
            log.error(f"Error fetching Klaviyo flow messages: {str(e)}")
            return []

    async def _fetch_flow_message_metrics(self, session: aiohttp.ClientSession, action_id: str) -> Dict:
        """Fetch metrics for a specific flow action using Metric Aggregates API"""
        try:
            # Initialize metrics
            total_metrics = {
                "recipients": 0,
                "opens": 0,
                "unique_opens": 0,
                "clicks": 0,
                "unique_clicks": 0,
                "unsubscribes": 0,
                "conversions": 0,
                "revenue": 0.0
            }

            # First get the flow messages under this action to get sent counts
            messages_url = f"{self.base_url}/flow-actions/{action_id}/flow-messages/"
            async with session.get(messages_url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    for msg in data.get("data", []):
                        attrs = msg.get("attributes", {})
                        total_metrics["recipients"] += attrs.get("sent_count", 0) or 0

            # Query metric aggregates for engagement metrics
            # Define metric names to query
            metrics_to_fetch = [
                ("Opened Email", "opens", "unique_opens"),
                ("Clicked Email", "clicks", "unique_clicks"),
                ("Unsubscribed", "unsubscribes", None),
            ]

            # Get the metric IDs first (cached if already fetched)
            if not hasattr(self, '_metric_ids_cache'):
                self._metric_ids_cache = await self._get_metric_ids(session)

            for metric_name, count_key, unique_key in metrics_to_fetch:
                metric_id = self._metric_ids_cache.get(metric_name)
                if not metric_id:
                    continue

                # Query aggregate for this flow action
                aggregate_url = f"{self.base_url}/metric-aggregates/"
                payload = {
                    "data": {
                        "type": "metric-aggregate",
                        "attributes": {
                            "metric_id": metric_id,
                            "measurements": ["count", "unique"] if unique_key else ["count"],
                            "filter": [f"equals($flow_action_id,\"{action_id}\")"],
                            "interval": "day",
                            "page_size": 1
                        }
                    }
                }

                headers = {**self.headers, "Content-Type": "application/json"}

                try:
                    async with session.post(aggregate_url, headers=headers, json=payload) as agg_resp:
                        if agg_resp.status == 200:
                            agg_data = await agg_resp.json()
                            results = agg_data.get("data", {}).get("attributes", {}).get("data", [])

                            for result in results:
                                measurements = result.get("measurements", {})
                                total_metrics[count_key] += measurements.get("count", 0) or 0
                                if unique_key:
                                    total_metrics[unique_key] += measurements.get("unique", 0) or 0
                except Exception as e:
                    log.debug(f"Error fetching {metric_name} aggregate for action {action_id}: {e}")

            return total_metrics

        except Exception as e:
            log.debug(f"Error fetching flow message metrics for {action_id}: {e}")
            return {}

    async def _get_metric_ids(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        """Get metric IDs for standard Klaviyo metrics"""
        metric_ids = {}
        try:
            url = f"{self.base_url}/metrics/"
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    for metric in data.get("data", []):
                        name = metric.get("attributes", {}).get("name")
                        if name:
                            metric_ids[name] = metric.get("id")
        except Exception as e:
            log.debug(f"Error fetching Klaviyo metric IDs: {e}")
        return metric_ids

    async def _fetch_flow_message_content(self, session: aiohttp.ClientSession, action_id: str) -> Dict:
        """Fetch content/subject for a flow message"""
        try:
            url = f"{self.base_url}/flow-actions/{action_id}/"

            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    attrs = data.get("data", {}).get("attributes", {})
                    settings = attrs.get("settings", {})

                    return {
                        "subject": settings.get("subject") or settings.get("template_subject"),
                        "from_name": settings.get("from_name"),
                        "from_email": settings.get("from_email")
                    }
                return {}

        except Exception as e:
            log.debug(f"Error fetching flow message content for {action_id}: {e}")
            return {}

    async def _fetch_metrics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch engagement metrics"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/metrics/"
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        metrics = []

                        for metric in data.get("data", []):
                            metrics.append({
                                "id": metric["id"],
                                "name": metric["attributes"].get("name"),
                                "integration": metric["attributes"].get("integration", {}).get("name"),
                                "created_at": metric["attributes"].get("created"),
                            })

                        log.info(f"Fetched {len(metrics)} metrics from Klaviyo")
                        return metrics

        except Exception as e:
            log.error(f"Error fetching Klaviyo metrics: {str(e)}")
            return []

    async def _fetch_lists(self) -> List[Dict]:
        """Fetch email lists"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/lists/"
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        lists = []

                        for lst in data.get("data", []):
                            lists.append({
                                "id": lst["id"],
                                "name": lst["attributes"].get("name"),
                                "created_at": lst["attributes"].get("created"),
                                "updated_at": lst["attributes"].get("updated"),
                            })

                        log.info(f"Fetched {len(lists)} lists from Klaviyo")
                        return lists

        except Exception as e:
            log.error(f"Error fetching Klaviyo lists: {str(e)}")
            return []

    async def _fetch_segments(self) -> List[Dict]:
        """Fetch customer segments"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/segments/"
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        segments = []

                        for segment in data.get("data", []):
                            segments.append({
                                "id": segment["id"],
                                "name": segment["attributes"].get("name"),
                                "created_at": segment["attributes"].get("created"),
                                "updated_at": segment["attributes"].get("updated"),
                            })

                        log.info(f"Fetched {len(segments)} segments from Klaviyo")
                        return segments

        except Exception as e:
            log.error(f"Error fetching Klaviyo segments: {str(e)}")
            return []
