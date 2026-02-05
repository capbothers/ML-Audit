"""
Base connector class for all data sources
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
from app.utils.logger import log
from app.utils.retry import RetryContext, is_retryable_error, calculate_backoff
import asyncio
import time


class BaseConnector(ABC):
    """Base class for all data source connectors"""

    # Retry configuration (can be overridden by subclasses)
    RETRY_MAX_ATTEMPTS = 3
    RETRY_BASE_DELAY = 2.0  # seconds
    RETRY_MAX_DELAY = 60.0  # seconds

    def __init__(self, name: str):
        self.name = name
        self.last_sync = None
        self.sync_count = 0
        self.error_count = 0
        self.retry_count = 0  # Total retries across all syncs

    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to data source"""
        pass

    @abstractmethod
    async def fetch_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Fetch data from source"""
        pass

    @abstractmethod
    async def validate_connection(self) -> bool:
        """Validate connection is working"""
        pass

    async def sync(self, start_date: datetime, end_date: datetime, **kwargs) -> Dict[str, Any]:
        """
        Sync data from source with error handling, retry logic, and logging

        Args:
            start_date: Start of date range
            end_date: End of date range
            **kwargs: Additional arguments passed to fetch_data (e.g., include_products=False)

        Returns:
            Dict with keys: success, source, data/error, sync_time, duration, retry_stats
        """
        log.info(f"Starting sync for {self.name} from {start_date} to {end_date}")
        start_time = time.time()
        # retries = number of extra attempts due to transient failures (0 = all succeeded first try)
        retry_stats = {"retries": 0, "total_delay_seconds": 0, "errors": []}

        try:
            # Validate connection first (with retry)
            connection_valid = await self._retry_operation(
                self.validate_connection,
                operation_name="validate_connection",
                retry_stats=retry_stats
            )

            if not connection_valid:
                raise ConnectionError(f"Connection validation failed for {self.name}")

            # Fetch data with retry (pass any extra kwargs like include_products)
            data = await self._retry_operation(
                lambda: self.fetch_data(start_date, end_date, **kwargs),
                operation_name="fetch_data",
                retry_stats=retry_stats
            )

            # Update metrics
            self.last_sync = datetime.utcnow()
            self.sync_count += 1

            elapsed = time.time() - start_time

            # Log with retry info if retries occurred
            if retry_stats["retries"] > 0:
                log.info(
                    f"Sync completed for {self.name} in {elapsed:.2f}s "
                    f"(after {retry_stats['retries']} retries, {retry_stats['total_delay_seconds']:.1f}s delay)"
                )
            else:
                log.info(f"Sync completed for {self.name} in {elapsed:.2f}s")

            return {
                "success": True,
                "source": self.name,
                "data": data,
                "sync_time": self.last_sync,
                "duration": elapsed,
                "retry_stats": retry_stats
            }

        except Exception as e:
            self.error_count += 1
            elapsed = time.time() - start_time
            log.error(f"Sync failed for {self.name} after {retry_stats['retries']} retries: {str(e)}")

            return {
                "success": False,
                "source": self.name,
                "error": str(e),
                "sync_time": datetime.utcnow(),
                "duration": elapsed,
                "retry_stats": retry_stats
            }

    async def _retry_operation(
        self,
        operation,
        operation_name: str = "operation",
        retry_stats: Optional[Dict] = None
    ) -> Any:
        """
        Execute an operation with retry logic.

        Args:
            operation: Async callable to execute
            operation_name: Name for logging
            retry_stats: Dict to track retry statistics (mutated in place)

        Returns:
            Result of the operation
        """
        last_error = None

        for attempt in range(1, self.RETRY_MAX_ATTEMPTS + 1):
            try:
                # Execute the operation
                result = operation()

                # Handle coroutines (from async functions or lambdas wrapping async calls)
                if asyncio.iscoroutine(result):
                    result = await result

                # Success - update retry count if we had retries
                if attempt > 1:
                    self.retry_count += (attempt - 1)

                return result

            except Exception as e:
                last_error = e

                # Check if we should retry
                if attempt >= self.RETRY_MAX_ATTEMPTS or not is_retryable_error(e):
                    if retry_stats:
                        retry_stats["errors"].append(f"{type(e).__name__}: {str(e)}")
                    raise

                # Calculate backoff delay
                delay = calculate_backoff(
                    attempt,
                    base_delay=self.RETRY_BASE_DELAY,
                    max_delay=self.RETRY_MAX_DELAY
                )

                # Track retry stats (cumulative across operations)
                if retry_stats:
                    retry_stats["retries"] += 1  # Count each retry (not first attempts)
                    retry_stats["total_delay_seconds"] += delay
                    retry_stats["errors"].append(f"{type(e).__name__}: {str(e)}")

                log.warning(
                    f"{self.name} {operation_name} attempt {attempt} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )

                await asyncio.sleep(delay)

        # Should not reach here
        raise last_error if last_error else RuntimeError("Retry exhausted")

    def get_status(self) -> Dict[str, Any]:
        """Get connector status"""
        return {
            "name": self.name,
            "last_sync": self.last_sync,
            "sync_count": self.sync_count,
            "error_count": self.error_count,
            "retry_count": self.retry_count,
            "error_rate": self.error_count / max(self.sync_count, 1),
            "retry_config": {
                "max_attempts": self.RETRY_MAX_ATTEMPTS,
                "base_delay": self.RETRY_BASE_DELAY,
                "max_delay": self.RETRY_MAX_DELAY
            }
        }
