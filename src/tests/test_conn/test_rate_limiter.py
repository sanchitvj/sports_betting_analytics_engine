import time
from betflow.api_connectors.conn_utils import RateLimiter


class TestRateLimiter:
    """Test suite for RateLimiter class."""

    def test_init(self):
        """Test RateLimiter initialization."""
        limiter = RateLimiter(requests_per_second=5)
        assert limiter.requests_per_second == 5
        assert len(limiter.request_times) == 0

    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        limiter = RateLimiter(requests_per_second=2)

        # First two requests should be immediate
        start_time = time.time()
        limiter.wait_if_needed()
        limiter.wait_if_needed()
        first_two_requests_time = time.time() - start_time

        assert first_two_requests_time < 0.1  # Should be nearly instant

        # Third request should wait
        start_time = time.time()
        limiter.wait_if_needed()
        wait_time = time.time() - start_time

        assert wait_time >= 0.9  # Should wait approximately 1 second

    def test_window_sliding(self):
        """Test sliding window behavior."""
        limiter = RateLimiter(requests_per_second=1)

        # Make first request
        limiter.wait_if_needed()

        # Wait for window to slide
        time.sleep(1.1)

        # Next request should be immediate
        start_time = time.time()
        limiter.wait_if_needed()
        execution_time = time.time() - start_time

        assert execution_time < 0.1