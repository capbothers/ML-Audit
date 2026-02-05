"""
Retry utilities with exponential backoff for API calls.

Provides decorators and context managers for handling transient failures.
"""
import asyncio
import functools
import random
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple, Type, Union
from app.utils.logger import log


@dataclass
class RetryStats:
    """Tracks retry statistics for a single operation."""
    attempts: int = 0
    total_delay_seconds: float = 0.0
    last_error: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    success: bool = False

    def record_attempt(self, error: Optional[Exception] = None, delay: float = 0.0):
        """Record a retry attempt."""
        self.attempts += 1
        self.total_delay_seconds += delay
        if error:
            error_str = f"{type(error).__name__}: {str(error)}"
            self.last_error = error_str
            self.errors.append(error_str)

    def mark_success(self):
        """Mark the operation as successful."""
        self.success = True

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/storage."""
        return {
            "attempts": self.attempts,
            "total_delay_seconds": round(self.total_delay_seconds, 2),
            "success": self.success,
            "last_error": self.last_error,
            "errors": self.errors[:5]  # Cap at 5 errors
        }


# Default retryable exceptions (network/API errors)
DEFAULT_RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,  # Includes network errors
)


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
) -> float:
    """
    Calculate delay for exponential backoff.

    Args:
        attempt: Current attempt number (1-indexed)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        exponential_base: Base for exponential calculation
        jitter: Add randomness to prevent thundering herd

    Returns:
        Delay in seconds
    """
    # Exponential backoff: base_delay * (exponential_base ^ (attempt - 1))
    delay = base_delay * (exponential_base ** (attempt - 1))

    # Cap at max_delay
    delay = min(delay, max_delay)

    # Add jitter (0-25% of delay)
    if jitter:
        jitter_amount = delay * random.uniform(0, 0.25)
        delay += jitter_amount

    return delay


def is_retryable_error(
    error: Exception,
    retryable_exceptions: Tuple[Type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
    retryable_status_codes: Tuple[int, ...] = (429, 500, 502, 503, 504)
) -> bool:
    """
    Check if an error is retryable.

    Args:
        error: The exception to check
        retryable_exceptions: Tuple of exception types to retry
        retryable_status_codes: HTTP status codes to retry (for HTTP errors)

    Returns:
        True if error should be retried
    """
    # Check exception type
    if isinstance(error, retryable_exceptions):
        return True

    # Check for HTTP errors with retryable status codes
    error_str = str(error).lower()

    # Check for rate limiting
    if "rate limit" in error_str or "too many requests" in error_str:
        return True

    # Check for common HTTP status codes in error messages
    for code in retryable_status_codes:
        if str(code) in error_str:
            return True

    # Check for timeout-related errors
    if "timeout" in error_str or "timed out" in error_str:
        return True

    # Check for connection-related errors
    if "connection" in error_str and ("refused" in error_str or "reset" in error_str or "failed" in error_str):
        return True

    return False


def retry_async(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None
):
    """
    Async decorator for retrying operations with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts
        base_delay: Initial delay between retries
        max_delay: Maximum delay cap
        exponential_base: Base for exponential backoff
        retryable_exceptions: Exception types to retry
        on_retry: Callback called on each retry (attempt, error, delay)

    Usage:
        @retry_async(max_attempts=3)
        async def fetch_api_data():
            ...
    """
    def decorator(func: Callable):
        # Mutable container to hold last call's stats (accessible from get_retry_stats)
        last_stats = [None]

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            stats = RetryStats()
            last_stats[0] = stats  # Store for get_retry_stats
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = await func(*args, **kwargs)
                    stats.record_attempt()
                    stats.mark_success()

                    # Log if we recovered from errors
                    if attempt > 1:
                        log.info(
                            f"{func.__name__} succeeded on attempt {attempt} "
                            f"after {stats.total_delay_seconds:.1f}s total delay"
                        )

                    return result

                except Exception as e:
                    last_error = e

                    # Check if we should retry
                    if attempt >= max_attempts or not is_retryable_error(e, retryable_exceptions):
                        stats.record_attempt(error=e)
                        log.error(
                            f"{func.__name__} failed after {attempt} attempts: {e}"
                        )
                        raise

                    # Calculate backoff delay
                    delay = calculate_backoff(
                        attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base
                    )

                    stats.record_attempt(error=e, delay=delay)

                    log.warning(
                        f"{func.__name__} attempt {attempt} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    # Call retry callback if provided
                    if on_retry:
                        on_retry(attempt, e, delay)

                    # Wait before retry
                    await asyncio.sleep(delay)

            # Should not reach here, but just in case
            raise last_error if last_error else RuntimeError("Retry exhausted")

        # Attach stats getter for testing/monitoring (returns stats from last call)
        wrapper.get_retry_stats = lambda: last_stats[0]
        return wrapper

    return decorator


def retry_sync(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None
):
    """
    Sync decorator for retrying operations with exponential backoff.

    Same as retry_async but for synchronous functions.
    """
    def decorator(func: Callable):
        # Mutable container to hold last call's stats (accessible from get_retry_stats)
        last_stats = [None]

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            stats = RetryStats()
            last_stats[0] = stats  # Store for get_retry_stats
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    stats.record_attempt()
                    stats.mark_success()

                    if attempt > 1:
                        log.info(
                            f"{func.__name__} succeeded on attempt {attempt} "
                            f"after {stats.total_delay_seconds:.1f}s total delay"
                        )

                    return result

                except Exception as e:
                    last_error = e

                    if attempt >= max_attempts or not is_retryable_error(e, retryable_exceptions):
                        stats.record_attempt(error=e)
                        log.error(
                            f"{func.__name__} failed after {attempt} attempts: {e}"
                        )
                        raise

                    delay = calculate_backoff(
                        attempt,
                        base_delay=base_delay,
                        max_delay=max_delay,
                        exponential_base=exponential_base
                    )

                    stats.record_attempt(error=e, delay=delay)

                    log.warning(
                        f"{func.__name__} attempt {attempt} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    if on_retry:
                        on_retry(attempt, e, delay)

                    time.sleep(delay)

            raise last_error if last_error else RuntimeError("Retry exhausted")

        # Attach stats getter for testing/monitoring (returns stats from last call)
        wrapper.get_retry_stats = lambda: last_stats[0]
        return wrapper

    return decorator


class RetryContext:
    """
    Context manager for retry operations with stats tracking.

    Usage:
        async with RetryContext(max_attempts=3) as ctx:
            result = await ctx.execute(api_call, arg1, arg2)
            print(ctx.stats.to_dict())
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retryable_exceptions: Tuple[Type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retryable_exceptions = retryable_exceptions
        self.stats = RetryStats()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def execute(self, func: Callable, *args, **kwargs):
        """Execute a function with retry logic."""
        last_error = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                self.stats.record_attempt()
                self.stats.mark_success()
                return result

            except Exception as e:
                last_error = e

                if attempt >= self.max_attempts or not is_retryable_error(e, self.retryable_exceptions):
                    self.stats.record_attempt(error=e)
                    raise

                delay = calculate_backoff(
                    attempt,
                    base_delay=self.base_delay,
                    max_delay=self.max_delay,
                    exponential_base=self.exponential_base
                )

                self.stats.record_attempt(error=e, delay=delay)

                log.warning(
                    f"Attempt {attempt} failed: {e}. Retrying in {delay:.1f}s..."
                )

                await asyncio.sleep(delay)

        raise last_error if last_error else RuntimeError("Retry exhausted")
