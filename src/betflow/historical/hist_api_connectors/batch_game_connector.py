from typing import Dict, Any, Optional
import requests

from betflow.api_connectors.conn_utils import RateLimiter


class ESPNBatchConnector:
    def __init__(self):
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports"
        self.rate_limiter = RateLimiter()
        self.session = requests.Session()

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Makes a rate-limited request to the ESPN API."""
        self.rate_limiter.wait_if_needed()
        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred: {e}")
