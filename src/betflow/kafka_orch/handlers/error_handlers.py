import logging
from datetime import timedelta
from typing import Optional, Dict
import requests


class KafkaErrorHandler:
    """Handles errors in Kafka operations."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.error_counts: Dict[str, int] = {}

    def handle_error(self, error: Exception, source: str) -> Optional[timedelta]:
        """Handle different types of errors and return retry delay if applicable."""
        self.error_counts[source] = self.error_counts.get(source, 0) + 1

        if isinstance(error, requests.exceptions.HTTPError):
            return self._handle_http_error(error, source)
        elif isinstance(error, requests.exceptions.ConnectionError):
            return self._handle_connection_error(source)
        elif isinstance(error, requests.exceptions.Timeout):
            return self._handle_timeout_error(source)

        self.logger.error(f"Unhandled error in {source}: {error}")
        return self._calculate_backoff(source)

    def _handle_http_error(self, error: requests.exceptions.HTTPError, source: str) -> Optional[timedelta]:
        """Handle HTTP-specific errors."""
        if error.response.status_code == 429:
            retry_after = int(error.response.headers.get('Retry-After', 3600))
            self.logger.warning(f"Rate limit exceeded for {source}, retrying after {retry_after} seconds")
            return timedelta(seconds=retry_after)
        elif error.response.status_code >= 500:
            self.logger.error(f"Server error for {source}: {error}")
            return self._calculate_backoff(source)
        return None

    def _handle_connection_error(self, source: str) -> timedelta:
        """Handle connection errors."""
        self.logger.error(f"Connection error for {source}")
        return self._calculate_backoff(source)

    def _handle_timeout_error(self, source: str) -> timedelta:
        """Handle timeout errors."""
        self.logger.error(f"Timeout error for {source}")
        return self._calculate_backoff(source)

    def _calculate_backoff(self, source: str) -> timedelta:
        """Calculate exponential backoff based on error count."""
        error_count = self.error_counts[source]
        delay = min(300, 2 ** error_count)  # Max 5 minutes
        return timedelta(seconds=delay)

    def reset_error_count(self, source: str) -> None:
        """Reset error count for a source after successful operation."""
        self.error_counts[source] = 0