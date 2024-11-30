import pytest
from datetime import timedelta
import requests
from betflow.kafka_orch.handlers import KafkaErrorHandler


class TestKafkaErrorHandler:
    @pytest.fixture
    def error_handler(self):
        return KafkaErrorHandler()

    def test_handle_http_error(self, error_handler):
        """Test handling of HTTP errors."""
        # Test rate limit error
        mock_429_error = requests.exceptions.HTTPError()
        mock_429_error.response = type('Response', (), {
            'status_code': 429,
            'headers': {'Retry-After': '60'}
        })()

        retry_after = error_handler.handle_error(mock_429_error, 'test_source')
        assert isinstance(retry_after, timedelta)
        assert retry_after.total_seconds() == 60

        # Test server error
        mock_500_error = requests.exceptions.HTTPError()
        mock_500_error.response = type('Response', (), {'status_code': 500})()

        retry_after = error_handler.handle_error(mock_500_error, 'test_source')
        assert isinstance(retry_after, timedelta)
        assert retry_after.total_seconds() > 0

    def test_handle_connection_error(self, error_handler):
        """Test handling of connection errors."""
        error = requests.exceptions.ConnectionError()
        retry_after = error_handler.handle_error(error, 'test_source')

        assert isinstance(retry_after, timedelta)
        assert retry_after.total_seconds() > 0

    def test_handle_timeout_error(self, error_handler):
        """Test handling of timeout errors."""
        error = requests.exceptions.Timeout()
        retry_after = error_handler.handle_error(error, 'test_source')

        assert isinstance(retry_after, timedelta)
        assert retry_after.total_seconds() > 0

    def test_error_count_tracking(self, error_handler):
        """Test error count tracking and reset."""
        source = 'test_source'

        # Test error count increment
        for i in range(3):
            error_handler.handle_error(Exception("Test error"), source)
            assert error_handler.error_counts[source] == i + 1

        # Test error count reset
        error_handler.reset_error_count(source)
        assert error_handler.error_counts[source] == 0

    def test_exponential_backoff(self, error_handler):
        """Test exponential backoff calculation."""
        source = 'test_source'

        # Test increasing backoff times
        previous_delay = 0
        for _ in range(3):
            retry_after = error_handler.handle_error(Exception("Test error"), source)
            current_delay = retry_after.total_seconds()
            assert current_delay > previous_delay
            previous_delay = current_delay

        # Test maximum backoff limit
        for _ in range(10):
            retry_after = error_handler.handle_error(Exception("Test error"), source)
            assert retry_after.total_seconds() <= 300  # Max 5 minutes