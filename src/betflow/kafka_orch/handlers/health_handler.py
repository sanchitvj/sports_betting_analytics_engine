import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from betflow.kafka_orch.monitoring.health_check import DataSourceStatus, DataSourceHealth


class HealthHandler:
    """Handles health status tracking for data sources."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sources: Dict[str, DataSourceHealth] = {}

    def initialize_source(self, source: str) -> None:
        """Initialize health status for a new source."""
        self.sources[source] = DataSourceHealth(
            status=DataSourceStatus.ACTIVE,
            last_success=None,
            failure_count=0,
            next_retry=None
        )
        self.logger.info(f"Initialized health tracking for {source}")

    def mark_source_failed(self, source: str) -> None:
        """Mark a source as failed during initialization."""
        self.sources[source] = DataSourceHealth(
            status=DataSourceStatus.FAILED,
            last_success=None,
            failure_count=1,
            next_retry=datetime.now() + timedelta(minutes=5)  # Default 5-minute retry
        )
        self.logger.warning(f"Marked {source} as failed during initialization")

    def can_use_source(self, source: str) -> bool:
        """Check if a source can be used based on its health status."""
        if source not in self.sources:
            self.logger.error(f"Unknown source: {source}")
            return False

        health = self.sources[source]

        # Can't use if failed
        if health.status == DataSourceStatus.FAILED:
            return False

        # Check if we need to wait before retrying
        if health.next_retry and datetime.now() < health.next_retry:
            return False

        return True

    def update_source_status(
        self,
        source: str,
        success: bool,
        error: Optional[Exception] = None,
        retry_after: Optional[timedelta] = None
    ) -> None:
        """Update health status for a source."""
        if source not in self.sources:
            self.logger.error(f"Attempting to update unknown source: {source}")
            return

        health = self.sources[source]

        if success:
            health.status = DataSourceStatus.ACTIVE
            health.last_success = datetime.now()
            health.failure_count = 0
            health.next_retry = None
            self.logger.debug(f"Source {source} health status updated: SUCCESS")
        else:
            health.failure_count += 1

            # Handle rate limiting
            if isinstance(error, Exception) and hasattr(error, 'response') and getattr(error.response, 'status_code',
                                                                                       None) == 429:
                health.status = DataSourceStatus.RATE_LIMITED
                health.next_retry = datetime.now() + retry_after if retry_after else datetime.now() + timedelta(hours=1)
                self.logger.warning(f"Source {source} rate limited, retry after: {health.next_retry}")
            else:
                health.status = DataSourceStatus.FAILED
                # Calculate exponential backoff if retry_after not provided
                if not retry_after:
                    retry_delay = min(300, 2 ** health.failure_count)  # Max 5 minutes
                    retry_after = timedelta(seconds=retry_delay)
                health.next_retry = datetime.now() + retry_after
                self.logger.error(f"Source {source} failed, retry after: {health.next_retry}")

    def get_source_health(self, source: str) -> Optional[DataSourceHealth]:
        """Get health status for a specific source."""
        return self.sources.get(source)

    def get_all_health_statuses(self) -> Dict[str, DataSourceHealth]:
        """Get health status for all sources."""
        return self.sources.copy()

    def reset_source_status(self, source: str) -> None:
        """Reset health status for a source."""
        if source in self.sources:
            self.sources[source] = DataSourceHealth(
                status=DataSourceStatus.ACTIVE,
                last_success=None,
                failure_count=0,
                next_retry=None
            )
            self.logger.info(f"Reset health status for {source}")