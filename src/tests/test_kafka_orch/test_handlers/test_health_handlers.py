import pytest
from datetime import datetime
from betflow.kafka_orch.handlers import HealthHandler
from betflow.kafka_orch.monitoring.health_check import DataSourceStatus
import requests


class TestHealthHandler:
    @pytest.fixture
    def health_handler(self):
        return HealthHandler()

    def test_initialize_source(self, health_handler):
        """Test source initialization."""
        health_handler.initialize_source('test_source')

        assert 'test_source' in health_handler.sources
        assert health_handler.sources['test_source'].status == DataSourceStatus.ACTIVE
        assert health_handler.sources['test_source'].failure_count == 0
        assert health_handler.sources['test_source'].next_retry is None

    def test_mark_source_failed(self, health_handler):
        """Test marking source as failed."""
        health_handler.mark_source_failed('test_source')

        assert 'test_source' in health_handler.sources
        assert health_handler.sources['test_source'].status == DataSourceStatus.FAILED
        assert health_handler.sources['test_source'].failure_count == 1
        assert health_handler.sources['test_source'].next_retry is not None

    def test_can_use_source(self, health_handler):
        """Test source availability check."""
        # Test active source
        health_handler.initialize_source('active_source')
        assert health_handler.can_use_source('active_source') is True

        # Test failed source
        health_handler.mark_source_failed('failed_source')
        assert health_handler.can_use_source('failed_source') is False

        # Test rate-limited source
        health_handler.initialize_source('rate_limited_source')
        mock_error = requests.exceptions.HTTPError()
        mock_error.response = type('Response', (), {'status_code': 429})()
        health_handler.update_source_status('rate_limited_source', False, mock_error)
        assert health_handler.can_use_source('rate_limited_source') is False

    def test_update_source_status(self, health_handler):
        """Test source status updates."""
        health_handler.initialize_source('test_source')

        # Test successful update
        health_handler.update_source_status('test_source', True)
        assert health_handler.sources['test_source'].status == DataSourceStatus.ACTIVE
        assert health_handler.sources['test_source'].failure_count == 0

        # Test failure update
        health_handler.update_source_status('test_source', False)
        assert health_handler.sources['test_source'].status == DataSourceStatus.FAILED
        assert health_handler.sources['test_source'].failure_count == 1

        # Test rate limit update
        mock_error = requests.exceptions.HTTPError()
        mock_error.response = type('Response', (), {'status_code': 429})()
        health_handler.update_source_status('test_source', False, mock_error)
        assert health_handler.sources['test_source'].status == DataSourceStatus.RATE_LIMITED

    def test_exponential_backoff(self, health_handler):
        """Test exponential backoff behavior."""
        health_handler.initialize_source('test_source')

        for i in range(3):  # Test multiple failures
            health_handler.update_source_status('test_source', False)
            expected_delay = min(300, 2 ** health_handler.sources['test_source'].failure_count)
            actual_delay = (health_handler.sources['test_source'].next_retry -
                            datetime.now()).total_seconds()
            assert abs(actual_delay - expected_delay) < 1  # Allow 1 second difference

    def test_reset_source_status(self, health_handler):
        """Test resetting source status."""
        health_handler.initialize_source('test_source')
        health_handler.update_source_status('test_source', False)

        health_handler.reset_source_status('test_source')
        assert health_handler.sources['test_source'].status == DataSourceStatus.ACTIVE
        assert health_handler.sources['test_source'].failure_count == 0
        assert health_handler.sources['test_source'].next_retry is None
