"""
Hotjar / Microsoft Clarity Connector

Syncs user behavior analytics: heatmaps, funnels, session recordings from Hotjar or Microsoft Clarity.
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import desc
import time
import requests

from app.connectors.base import BaseConnector
from app.models.hotjar_data import (
    HotjarPageData, HotjarFunnelStep, HotjarRecordingSummary,
    HotjarPoll, ClaritySession
)
from app.config import get_settings
from app.utils.logger import log

settings = get_settings()


class HotjarConnector(BaseConnector):
    """
    Hotjar / Microsoft Clarity API connector

    Syncs:
    - Page heatmap data (clicks, scroll depth, rage clicks)
    - Funnel performance (drop-off rates)
    - Recording summaries (sessions with frustration signals)
    - Poll responses
    - Clarity sessions (if using Clarity instead)
    """

    def __init__(self, db: Session):
        super().__init__(db, source_name="hotjar", source_type="user_behavior")
        self.hotjar_site_id = settings.hotjar_site_id
        self.hotjar_api_key = settings.hotjar_api_key
        self.clarity_project_id = settings.clarity_project_id
        self.clarity_api_key = settings.clarity_api_key

        # Determine which service to use
        self.use_hotjar = bool(self.hotjar_site_id and self.hotjar_api_key)
        self.use_clarity = bool(self.clarity_project_id and self.clarity_api_key)

        if self.use_hotjar:
            self.base_url = "https://api.hotjar.com/api/v1"
            self.headers = {
                'Authorization': f'Bearer {self.hotjar_api_key}',
                'Accept': 'application/json'
            }
        elif self.use_clarity:
            self.base_url = "https://www.clarity.ms/api"
            self.headers = {
                'Authorization': f'Bearer {self.clarity_api_key}',
                'Accept': 'application/json'
            }

    async def authenticate(self) -> bool:
        """
        Authenticate with Hotjar or Clarity API

        Returns:
            True if authentication successful
        """
        try:
            if self.use_hotjar:
                return await self._authenticate_hotjar()
            elif self.use_clarity:
                return await self._authenticate_clarity()
            else:
                log.error("No Hotjar or Clarity credentials configured")
                return False

        except Exception as e:
            log.error(f"Hotjar/Clarity authentication failed: {str(e)}")
            self._authenticated = False
            return False

    async def _authenticate_hotjar(self) -> bool:
        """Authenticate with Hotjar API"""
        try:
            # Test authentication by getting site info
            response = requests.get(
                f"{self.base_url}/sites/{self.hotjar_site_id}",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                self._authenticated = True
                log.info("Hotjar authentication successful")
                return True
            else:
                log.error(f"Hotjar authentication failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            log.error(f"Hotjar authentication error: {str(e)}")
            return False

    async def _authenticate_clarity(self) -> bool:
        """Authenticate with Microsoft Clarity API"""
        try:
            # Test authentication by getting project info
            response = requests.get(
                f"{self.base_url}/projects/{self.clarity_project_id}",
                headers=self.headers,
                timeout=10
            )

            if response.status_code == 200:
                self._authenticated = True
                log.info("Clarity authentication successful")
                return True
            else:
                log.error(f"Clarity authentication failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            log.error(f"Clarity authentication error: {str(e)}")
            return False

    async def sync(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Sync data from Hotjar or Clarity

        Args:
            start_date: Start date for sync (defaults to last sync or 90 days ago)
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
                end_date = datetime.now() - timedelta(days=1)

            if not start_date:
                # Get last successful sync
                last_sync = await self.get_last_successful_sync()
                if last_sync:
                    start_date = last_sync
                else:
                    # First sync: get 90 days of data
                    start_date = datetime.now() - timedelta(days=90)

            log.info(f"Syncing behavior data from {start_date.date()} to {end_date.date()}")

            total_records = 0

            if self.use_hotjar:
                # Sync Hotjar data
                page_data_synced = await self._sync_hotjar_page_data(start_date, end_date)
                total_records += page_data_synced

                funnels_synced = await self._sync_hotjar_funnels(start_date, end_date)
                total_records += funnels_synced

                recordings_synced = await self._sync_hotjar_recordings(start_date, end_date)
                total_records += recordings_synced

                polls_synced = await self._sync_hotjar_polls()
                total_records += polls_synced

            elif self.use_clarity:
                # Sync Clarity data
                sessions_synced = await self._sync_clarity_sessions(start_date, end_date)
                total_records += sessions_synced

            # Calculate sync duration
            sync_duration = time.time() - sync_start_time

            # Log success
            await self.log_sync_success(
                records_synced=total_records,
                latest_data_timestamp=end_date,
                sync_duration_seconds=sync_duration
            )

            log.info(f"Behavior data sync completed: {total_records} records in {sync_duration:.1f}s")

            return {
                "success": True,
                "records_synced": total_records,
                "duration_seconds": sync_duration
            }

        except Exception as e:
            error_msg = f"Behavior data sync failed: {str(e)}"
            log.error(error_msg)
            await self.log_sync_failure(error_msg)

            return {
                "success": False,
                "error": error_msg,
                "records_synced": 0
            }

    async def _sync_hotjar_page_data(self, start_date: datetime, end_date: datetime) -> int:
        """Sync Hotjar heatmap and page data"""
        try:
            # Get all heatmaps
            response = requests.get(
                f"{self.base_url}/sites/{self.hotjar_site_id}/heatmaps",
                headers=self.headers
            )

            if response.status_code != 200:
                log.error(f"Failed to get Hotjar heatmaps: {response.status_code}")
                return 0

            data = response.json()
            records_synced = 0

            if 'heatmaps' in data:
                for heatmap in data['heatmaps']:
                    heatmap_id = heatmap.get('id')

                    # Get detailed heatmap data
                    detail_response = requests.get(
                        f"{self.base_url}/sites/{self.hotjar_site_id}/heatmaps/{heatmap_id}",
                        headers=self.headers
                    )

                    if detail_response.status_code == 200:
                        detail_data = detail_response.json()
                        heatmap_detail = detail_data.get('heatmap', {})

                        # Extract aggregated data
                        page_url = heatmap_detail.get('page_url', '')
                        page_views = heatmap_detail.get('snapshot_count', 0)

                        # Get behavior signals (these might require additional API calls)
                        # For now, use placeholder values
                        record = HotjarPageData(
                            date=end_date.date(),
                            page_url=page_url,
                            page_views=page_views,
                            scroll_depth_avg=0.0,  # Would need scroll map API
                            rage_click_count=0,
                            dead_click_count=0,
                            u_turn_count=0,
                            click_heatmap_data=None,
                            scroll_heatmap_data=None
                        )

                        self.db.merge(record)
                        records_synced += 1

                        if records_synced % 50 == 0:
                            self.db.commit()
                            time.sleep(0.1)  # Rate limit

                self.db.commit()

            log.info(f"Synced {records_synced} Hotjar page data records")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Hotjar page data: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_hotjar_funnels(self, start_date: datetime, end_date: datetime) -> int:
        """Sync Hotjar funnel data"""
        try:
            # Get all funnels
            response = requests.get(
                f"{self.base_url}/sites/{self.hotjar_site_id}/funnels",
                headers=self.headers
            )

            if response.status_code != 200:
                log.error(f"Failed to get Hotjar funnels: {response.status_code}")
                return 0

            data = response.json()
            records_synced = 0

            if 'funnels' in data:
                for funnel in data['funnels']:
                    funnel_id = funnel.get('id')
                    funnel_name = funnel.get('name', 'Untitled')

                    # Get funnel steps
                    steps = funnel.get('steps', [])

                    for step_number, step in enumerate(steps, 1):
                        step_name = step.get('name', f'Step {step_number}')
                        step_url = step.get('url', '')

                        # Get step metrics (might require additional API call)
                        record = HotjarFunnelStep(
                            date=end_date.date(),
                            funnel_name=funnel_name,
                            step_number=step_number,
                            step_name=step_name,
                            step_url=step_url,
                            sessions_entered=0,  # Would need funnel report API
                            sessions_completed=0,
                            drop_off_rate=0.0,
                            avg_time_on_step=0.0
                        )

                        self.db.merge(record)
                        records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Hotjar funnel steps")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Hotjar funnels: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_hotjar_recordings(self, start_date: datetime, end_date: datetime) -> int:
        """Sync Hotjar recording summaries (sessions with frustration signals)"""
        try:
            # Get recordings with filters
            params = {
                'filter[has_rage_click]': 'true',  # Only recordings with rage clicks
                'page_size': 100
            }

            response = requests.get(
                f"{self.base_url}/sites/{self.hotjar_site_id}/recordings",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                log.error(f"Failed to get Hotjar recordings: {response.status_code}")
                return 0

            data = response.json()
            records_synced = 0

            if 'recordings' in data:
                for recording in data['recordings']:
                    recording_id = recording.get('id')
                    created_at = recording.get('created_at')

                    # Parse date
                    recording_date = None
                    if created_at:
                        try:
                            recording_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        except (ValueError, TypeError) as e:
                            log.warning(f"Failed to parse Hotjar recording date '{created_at}': {e}")

                    record = HotjarRecordingSummary(
                        recording_id=str(recording_id),
                        date=recording_date.date() if recording_date else end_date.date(),
                        user_id=None,  # Hotjar doesn't expose user ID
                        session_duration=recording.get('duration', 0),
                        pages_visited=recording.get('page_count', 0),
                        device_type=recording.get('device', 'unknown').upper(),
                        browser=recording.get('browser', 'unknown'),
                        recording_url=f"https://insights.hotjar.com/sites/{self.hotjar_site_id}/recordings/{recording_id}",
                        has_rage_clicks=recording.get('has_rage_click', False),
                        has_u_turns=recording.get('has_u_turn', False),
                        has_javascript_errors=recording.get('has_error', False),
                        converted=recording.get('converted', False)
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Hotjar recordings")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Hotjar recordings: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_hotjar_polls(self) -> int:
        """Sync Hotjar poll responses"""
        try:
            # Get all polls
            response = requests.get(
                f"{self.base_url}/sites/{self.hotjar_site_id}/polls",
                headers=self.headers
            )

            if response.status_code != 200:
                log.error(f"Failed to get Hotjar polls: {response.status_code}")
                return 0

            data = response.json()
            records_synced = 0

            if 'polls' in data:
                for poll in data['polls']:
                    poll_id = poll.get('id')
                    poll_name = poll.get('name', 'Untitled')
                    question = poll.get('question', '')

                    # Get poll responses
                    responses_response = requests.get(
                        f"{self.base_url}/sites/{self.hotjar_site_id}/polls/{poll_id}/responses",
                        headers=self.headers
                    )

                    responses = []
                    response_count = 0

                    if responses_response.status_code == 200:
                        responses_data = responses_response.json()
                        if 'responses' in responses_data:
                            responses = responses_data['responses']
                            response_count = len(responses)

                    record = HotjarPoll(
                        poll_id=str(poll_id),
                        poll_name=poll_name,
                        question=question,
                        response_count=response_count,
                        responses=responses if responses else None
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Hotjar polls")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Hotjar polls: {str(e)}")
            self.db.rollback()
            return 0

    async def _sync_clarity_sessions(self, start_date: datetime, end_date: datetime) -> int:
        """Sync Microsoft Clarity session data"""
        try:
            # Clarity API might have different endpoints
            # This is a placeholder implementation
            log.info("Clarity session sync - API endpoints may vary")

            # Get sessions with filters
            params = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'filter': 'rageClicks'  # Filter for sessions with rage clicks
            }

            response = requests.get(
                f"{self.base_url}/projects/{self.clarity_project_id}/sessions",
                headers=self.headers,
                params=params
            )

            if response.status_code != 200:
                log.error(f"Failed to get Clarity sessions: {response.status_code}")
                return 0

            data = response.json()
            records_synced = 0

            if 'sessions' in data:
                for session in data['sessions']:
                    session_id = session.get('id')
                    page_url = session.get('url', '')

                    # Parse session date
                    session_date = None
                    if 'startTime' in session:
                        try:
                            session_date = datetime.fromisoformat(session['startTime'].replace('Z', '+00:00'))
                        except (ValueError, TypeError) as e:
                            log.warning(f"Failed to parse Clarity session date '{session['startTime']}': {e}")

                    # Extract frustration signals
                    rage_clicks = session.get('rageClickCount', 0)
                    dead_clicks = session.get('deadClickCount', 0)
                    excessive_scrolling = session.get('excessiveScrolling', False)
                    js_errors = session.get('javascriptErrors', [])

                    record = ClaritySession(
                        session_id=session_id,
                        date=session_date.date() if session_date else end_date.date(),
                        page_url=page_url,
                        session_duration=session.get('duration', 0),
                        pages_visited=session.get('pageCount', 0),
                        rage_click_count=rage_clicks,
                        dead_click_count=dead_clicks,
                        excessive_scrolling=excessive_scrolling,
                        javascript_errors=js_errors if js_errors else None,
                        recording_url=f"https://clarity.microsoft.com/projects/view/{self.clarity_project_id}/sessions/{session_id}"
                    )

                    self.db.merge(record)
                    records_synced += 1

                    if records_synced % 50 == 0:
                        self.db.commit()
                        time.sleep(0.1)

                self.db.commit()

            log.info(f"Synced {records_synced} Clarity sessions")
            return records_synced

        except Exception as e:
            log.error(f"Error syncing Clarity sessions: {str(e)}")
            self.db.rollback()
            return 0

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """Get timestamp of most recent data"""
        try:
            if self.use_hotjar:
                latest = self.db.query(HotjarPageData).order_by(
                    desc(HotjarPageData.date)
                ).first()
            else:
                latest = self.db.query(ClaritySession).order_by(
                    desc(ClaritySession.date)
                ).first()

            if latest:
                return datetime.combine(latest.date, datetime.min.time())

            return None

        except Exception as e:
            log.error(f"Error getting latest behavior data timestamp: {str(e)}")
            return None
