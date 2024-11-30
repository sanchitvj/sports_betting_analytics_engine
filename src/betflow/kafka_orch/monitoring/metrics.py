import logging
from typing import Dict, Any
import json
import threading
import time


class MetricsCollector:
    """Collects and manages metrics for Kafka operations."""

    def __init__(self, metrics_interval: int = 60):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.metrics_interval = metrics_interval
        self.metrics: Dict[str, Any] = {
            "total_messages": 0,
            "failed_messages": 0,
            "success_messages": 0,
            "rate_limit_hits": 0,
            "source_metrics": {},
            "latency_metrics": {
                "min": float("inf"),
                "max": 0,
                "avg": 0,
                "total": 0,
                "count": 0,
            },
        }
        self._start_metrics_reporter()

    def _initialize_source_metrics(self, source: str) -> None:
        """Initialize metrics for a new source."""
        if source not in self.metrics["source_metrics"]:
            self.metrics["source_metrics"][source] = {
                "success": 0,
                "failed": 0,
                "rate_limits": 0,
            }

    def record_message(self, source: str, success: bool, latency: float) -> None:
        """Record message metrics."""
        self.metrics["total_messages"] += 1

        if success:
            self.metrics["success_messages"] += 1
        else:
            self.metrics["failed_messages"] += 1

        # Initialize source metrics if needed
        self._initialize_source_metrics(source)

        # Update source-specific metrics
        self.metrics["source_metrics"][source]["success" if success else "failed"] += 1

        # Update latency metrics
        self._update_latency_metrics(latency)

    def record_rate_limit(self, source: str) -> None:
        """Record rate limit hit."""
        self.metrics["rate_limit_hits"] += 1

        # Initialize source metrics if needed
        self._initialize_source_metrics(source)

        self.metrics["source_metrics"][source]["rate_limits"] += 1

    def _update_latency_metrics(self, latency: float) -> None:
        """Update latency statistics."""
        metrics = self.metrics["latency_metrics"]
        metrics["min"] = min(metrics["min"], latency)
        metrics["max"] = max(metrics["max"], latency)
        metrics["total"] += latency
        metrics["count"] += 1
        metrics["avg"] = metrics["total"] / metrics["count"]

    def _start_metrics_reporter(self) -> None:
        """Start periodic metrics reporting."""

        def report_metrics():
            while True:
                self.log_metrics()
                time.sleep(self.metrics_interval)

        thread = threading.Thread(target=report_metrics, daemon=True)
        thread.start()

    def log_metrics(self) -> None:
        """Log current metrics."""
        self.logger.info(f"Current Metrics: {json.dumps(self.metrics, indent=2)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return self.metrics.copy()

    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self.metrics = {
            "total_messages": 0,
            "failed_messages": 0,
            "success_messages": 0,
            "rate_limit_hits": 0,
            "source_metrics": {},
            "latency_metrics": {
                "min": float("inf"),
                "max": 0,
                "avg": 0,
                "total": 0,
                "count": 0,
            },
        }
