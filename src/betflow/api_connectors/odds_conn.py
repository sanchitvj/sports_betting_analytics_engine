import json
import time
from typing import Dict, Any, Optional
import requests
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter


class OddsAPIConnector:
    """Connector for The Odds API with Kafka integration."""

    def __init__(
        self,
        api_key: str,
        kafka_bootstrap_servers: str,
        base_url: str = "https://api.the-odds-api.com/v4",
    ) -> None:
        """Initialize the connector.

        Args:
            api_key: The Odds API key
            kafka_bootstrap_servers: Kafka bootstrap servers
            base_url: Base URL for The Odds API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.rate_limiter = RateLimiter(requests_per_second=1)  # Free tier limit

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to The Odds API.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            API response as dictionary

        Raises:
            Exception: If request fails
        """
        self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}/{endpoint}"
        headers = {"apikey": self.api_key}

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()

            # Check remaining requests
            requests_remaining = response.headers.get("x-requests-remaining")
            if requests_remaining and int(requests_remaining) < 10:
                print(f"Warning: Only {requests_remaining} API requests remaining")

            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 3600))
                raise Exception(
                    f"Rate limit exceeded. Retry after {retry_after} seconds"
                )
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    @staticmethod
    def transform_odds_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw odds data to our schema.

        Args:
            raw_data: Raw data from API

        Returns:
            Transformed data
        """
        return {
            "game_id": raw_data.get("id"),
            "sport_key": raw_data.get("sport_key"),
            "sport_title": raw_data.get("sport_title"),
            "commence_time": raw_data.get("commence_time"),
            "home_team": raw_data.get("home_team"),
            "away_team": raw_data.get("away_team"),
            "bookmakers": [
                {
                    "key": bm.get("key"),
                    "title": bm.get("title"),
                    "last_update": bm.get("last_update"),
                    "markets": [
                        {
                            "key": market.get("key"),
                            "outcomes": [
                                {
                                    "name": outcome.get("name"),
                                    "price": outcome.get("price"),
                                }
                                for outcome in market.get("outcomes", [])
                            ],
                        }
                        for market in bm.get("markets", [])
                    ],
                }
                for bm in raw_data.get("bookmakers", [])
            ],
            "timestamp": int(time.time()),
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic.

        Args:
            topic: Kafka topic
            data: Data to publish

        Raises:
            Exception: If publishing fails
        """
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    async def fetch_and_publish_odds(
        self,
        sport: str,
        topic_name: str,
        markets: str = "h2h",
        regions: str = "us",
        odds_format: str = "decimal",
    ) -> None:
        """Fetch odds for a sport and publish to Kafka.

        Args:
            sport: Sport key (e.g., 'basketball_nba', americanfootball_ncaaf, americanfootball_nfl, icehockey_nhl)
            topic_name (str): kafka topic name
            markets: Odds market types (h2h, spreads, total)
            regions: Regions for odds
            odds_format: Format for odds values
        """
        try:
            params = {
                "api_key": self.api_key,
                "regions": regions,
                "markets": markets,
                # "oddsFormat": odds_format,
            }

            raw_data = self.make_request(f"sports/{sport}/odds", params=params)

            transformed_data = self.transform_odds_data(raw_data)
            self.publish_to_kafka(topic_name, transformed_data)

        except Exception as e:
            raise Exception(f"Failed to fetch and publish odds: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


# def main():
#     """Main function to demonstrate usage."""
#     api_key = os.getenv("ODDS_API_KEY")
#     connector = OddsAPIConnector(
#         api_key=api_key, kafka_bootstrap_servers="localhost:9092"
#     )
#
#     try:
#         # Fetch NBA odds
#         connector.fetch_and_publish_odds("basketball_nba")
#
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         connector.close()
#
#
# if __name__ == "__main__":
#     main()
