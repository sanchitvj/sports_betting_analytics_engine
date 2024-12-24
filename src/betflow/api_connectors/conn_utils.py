from datetime import datetime, timedelta
from collections import deque
import time


class RateLimiter:
    """Rate limiter implementation using sliding window algorithm.

    Args:
        requests_per_second (int): Maximum number of requests allowed per second.

    Attributes:
        requests_per_second (int): Maximum requests allowed per second.
        request_times (collections.deque): Queue to track request timestamps.
    """

    def __init__(self, requests_per_second: int = 2) -> None:
        self.requests_per_second = requests_per_second
        self.request_times = deque()

    def wait_if_needed(self) -> None:
        """Implements rate limiting by waiting if request limit is reached.

        Checks if the number of requests in the current sliding window exceeds
        the limit and waits if necessary.
        """
        now = datetime.now()

        # Remove old requests from the window
        while self.request_times and self.request_times[0] < now - timedelta(seconds=1):
            self.request_times.popleft()

        # If we've hit the limit, wait
        if len(self.request_times) >= self.requests_per_second:
            sleep_time = (
                self.request_times[0] + timedelta(seconds=3) - now
            ).total_seconds()
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.request_times.append(now)
