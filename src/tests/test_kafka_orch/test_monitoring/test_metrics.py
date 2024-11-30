import pytest
from betflow.kafka_orch.monitoring.metrics import MetricsCollector


class TestMetricsCollector:
    @pytest.fixture
    def metrics_collector(self):
        return MetricsCollector(metrics_interval=1)

    def test_record_message_success(self, metrics_collector):
        """Test recording successful message metrics."""
        metrics_collector.record_message("test_source", True, 0.5)

        metrics = metrics_collector.get_metrics()
        assert metrics["total_messages"] == 1
        assert metrics["success_messages"] == 1
        assert metrics["failed_messages"] == 0
        assert metrics["source_metrics"]["test_source"]["success"] == 1
        assert metrics["latency_metrics"]["avg"] == 0.5

    def test_record_message_failure(self, metrics_collector):
        """Test recording failed message metrics."""
        metrics_collector.record_message("test_source", False, 0)

        metrics = metrics_collector.get_metrics()
        assert metrics["total_messages"] == 1
        assert metrics["success_messages"] == 0
        assert metrics["failed_messages"] == 1
        assert metrics["source_metrics"]["test_source"]["failed"] == 1

    def test_record_rate_limit(self, metrics_collector):
        """Test recording rate limit hits."""
        metrics_collector.record_rate_limit("test_source")

        metrics = metrics_collector.get_metrics()
        assert metrics["rate_limit_hits"] == 1
        assert metrics["source_metrics"]["test_source"]["rate_limits"] == 1

    def test_latency_metrics(self, metrics_collector):
        """Test latency statistics calculation."""
        latencies = [0.1, 0.2, 0.3]
        for latency in latencies:
            metrics_collector.record_message("test_source", True, latency)

        metrics = metrics_collector.get_metrics()
        assert metrics["latency_metrics"]["min"] == 0.1
        assert metrics["latency_metrics"]["max"] == 0.3
        assert round(metrics["latency_metrics"]["avg"], 2) == 0.2
        assert metrics["latency_metrics"]["count"] == 3

    def test_reset_metrics(self, metrics_collector):
        """Test metrics reset functionality."""
        metrics_collector.record_message("test_source", True, 0.5)
        metrics_collector.reset_metrics()

        metrics = metrics_collector.get_metrics()
        assert metrics["total_messages"] == 0
        assert metrics["success_messages"] == 0
        assert metrics["failed_messages"] == 0
        assert metrics["rate_limit_hits"] == 0
        assert not metrics["source_metrics"]
        assert metrics["latency_metrics"]["count"] == 0

    def test_multiple_sources(self, metrics_collector):
        """Test handling multiple sources."""
        metrics_collector.record_message("source1", True, 0.1)
        metrics_collector.record_message("source2", False, 0.2)
        metrics_collector.record_rate_limit("source1")

        metrics = metrics_collector.get_metrics()
        assert "source1" in metrics["source_metrics"]
        assert "source2" in metrics["source_metrics"]
        assert metrics["source_metrics"]["source1"]["success"] == 1
        assert metrics["source_metrics"]["source2"]["failed"] == 1
        assert metrics["source_metrics"]["source1"]["rate_limits"] == 1
