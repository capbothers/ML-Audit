"""
Base Connector Class

All data connectors inherit from this base class.
Provides common functionality for authentication, sync logging, and error handling.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.models.data_quality import DataSyncStatus
from app.utils.logger import log


class BaseConnector(ABC):
    """
    Base class for all data source connectors

    Implements common patterns:
    - Sync status logging
    - Error handling
    - Incremental sync tracking
    - Rate limit management
    """

    def __init__(self, db: Session, source_name: str, source_type: str):
        """
        Initialize connector

        Args:
            db: Database session
            source_name: Name of data source (e.g., 'shopify', 'google_ads')
            source_type: Type of source (e.g., 'ecommerce', 'advertising', 'analytics')
        """
        self.db = db
        self.source_name = source_name
        self.source_type = source_type
        self._authenticated = False

    @abstractmethod
    async def authenticate(self) -> bool:
        """
        Authenticate with the data source

        Returns:
            True if authentication successful, False otherwise
        """
        pass

    @abstractmethod
    async def sync(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Run incremental sync

        Args:
            start_date: Start date for sync (if None, use last successful sync)
            end_date: End date for sync (if None, use current time)

        Returns:
            Dict with sync results (records_synced, errors, etc.)
        """
        pass

    async def get_last_successful_sync(self) -> Optional[datetime]:
        """
        Get timestamp of last successful sync

        Returns:
            Datetime of last successful sync, or None if never synced
        """
        try:
            status = self.db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name == self.source_name,
                DataSyncStatus.sync_status == 'success'
            ).order_by(
                desc(DataSyncStatus.last_successful_sync)
            ).first()

            return status.last_successful_sync if status else None

        except Exception as e:
            log.error(f"Error getting last sync for {self.source_name}: {str(e)}")
            return None

    async def get_latest_data_timestamp(self) -> Optional[datetime]:
        """
        Get timestamp of most recent data point synced

        Subclasses can override this to query their specific tables

        Returns:
            Datetime of latest data point, or None
        """
        status = self.db.query(DataSyncStatus).filter(
            DataSyncStatus.source_name == self.source_name
        ).order_by(
            desc(DataSyncStatus.latest_data_timestamp)
        ).first()

        return status.latest_data_timestamp if status else None

    async def log_sync_start(self) -> DataSyncStatus:
        """
        Log that sync has started

        Returns:
            DataSyncStatus record
        """
        try:
            # Get or create status record
            status = self.db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name == self.source_name
            ).first()

            if not status:
                status = DataSyncStatus(
                    source_name=self.source_name,
                    source_type=self.source_type
                )
                self.db.add(status)

            # Update to in_progress
            status.last_sync_attempt = datetime.utcnow()
            status.sync_status = 'in_progress'

            self.db.commit()

            log.info(f"Started sync for {self.source_name}")

            return status

        except Exception as e:
            log.error(f"Error logging sync start for {self.source_name}: {str(e)}")
            self.db.rollback()
            raise

    async def log_sync_success(
        self,
        records_synced: int,
        latest_data_timestamp: Optional[datetime] = None,
        sync_duration_seconds: Optional[float] = None
    ):
        """
        Log successful sync completion

        Args:
            records_synced: Number of records synced
            latest_data_timestamp: Timestamp of most recent data point
            sync_duration_seconds: How long sync took
        """
        try:
            status = self.db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name == self.source_name
            ).first()

            if not status:
                log.warning(f"No status record found for {self.source_name}, creating new one")
                status = DataSyncStatus(
                    source_name=self.source_name,
                    source_type=self.source_type
                )
                self.db.add(status)

            # Update status
            now = datetime.utcnow()
            status.last_successful_sync = now
            status.sync_status = 'success'
            status.records_synced = records_synced
            status.records_failed = 0
            status.last_error = None
            status.error_count = 0  # Reset error count on success
            status.first_error_at = None

            if latest_data_timestamp:
                status.latest_data_timestamp = latest_data_timestamp
                # Calculate data lag
                status.data_lag_hours = (now - latest_data_timestamp).total_seconds() / 3600

            if sync_duration_seconds:
                status.sync_duration_seconds = sync_duration_seconds

            # Update health
            status.is_healthy = True
            status.health_score = 100
            status.health_issues = None

            self.db.commit()

            log.info(
                f"Sync successful for {self.source_name}: "
                f"{records_synced} records, "
                f"{sync_duration_seconds:.1f}s" if sync_duration_seconds else ""
            )

        except Exception as e:
            log.error(f"Error logging sync success for {self.source_name}: {str(e)}")
            self.db.rollback()

    async def log_sync_failure(
        self,
        error_message: str,
        records_failed: int = 0
    ):
        """
        Log failed sync

        Args:
            error_message: Error description
            records_failed: Number of records that failed to sync
        """
        try:
            status = self.db.query(DataSyncStatus).filter(
                DataSyncStatus.source_name == self.source_name
            ).first()

            if not status:
                status = DataSyncStatus(
                    source_name=self.source_name,
                    source_type=self.source_type
                )
                self.db.add(status)

            # Update status
            status.sync_status = 'failed'
            status.last_error = error_message[:500]  # Truncate if too long
            status.records_failed = records_failed
            status.error_count = (status.error_count or 0) + 1

            if not status.first_error_at:
                status.first_error_at = datetime.utcnow()

            # Update health based on consecutive errors
            if status.error_count >= 5:
                status.is_healthy = False
                status.health_score = 0
                status.health_issues = [
                    f"Consecutive failures: {status.error_count}",
                    f"Last error: {error_message[:100]}"
                ]
            elif status.error_count >= 3:
                status.is_healthy = False
                status.health_score = 30
                status.health_issues = [
                    f"Multiple failures: {status.error_count}",
                    f"Last error: {error_message[:100]}"
                ]
            else:
                status.health_score = max(0, 100 - (status.error_count * 20))

            self.db.commit()

            log.error(
                f"Sync failed for {self.source_name}: {error_message} "
                f"(consecutive failures: {status.error_count})"
            )

        except Exception as e:
            log.error(f"Error logging sync failure for {self.source_name}: {str(e)}")
            self.db.rollback()

    async def check_stale_data(self, max_age_hours: int = 24) -> bool:
        """
        Check if data is stale (no sync in X hours)

        Args:
            max_age_hours: Maximum acceptable age in hours

        Returns:
            True if data is stale, False otherwise
        """
        last_sync = await self.get_last_successful_sync()

        if not last_sync:
            return True  # Never synced = stale

        hours_since_sync = (datetime.utcnow() - last_sync).total_seconds() / 3600

        is_stale = hours_since_sync > max_age_hours

        if is_stale:
            log.warning(
                f"{self.source_name} data is stale: "
                f"{hours_since_sync:.1f} hours since last sync"
            )

        return is_stale

    def is_authenticated(self) -> bool:
        """Check if connector is authenticated"""
        return self._authenticated

    def _handle_rate_limit(self, retry_after: int):
        """
        Handle rate limit response

        Args:
            retry_after: Seconds to wait before retrying
        """
        log.warning(f"{self.source_name} rate limited, waiting {retry_after}s")
        # Subclasses can implement actual rate limit handling
        # For now, just log it

    def _normalize_date(self, date_value: Any) -> Optional[datetime]:
        """
        Normalize various date formats to datetime

        Args:
            date_value: Date in various formats (str, datetime, etc.)

        Returns:
            Normalized datetime or None
        """
        if not date_value:
            return None

        if isinstance(date_value, datetime):
            return date_value

        if isinstance(date_value, str):
            # Try common formats
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue

            log.warning(f"Could not parse date: {date_value}")
            return None

        return None
