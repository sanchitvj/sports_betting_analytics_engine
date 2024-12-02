import json
import time
from typing import Dict, Any, Optional, List
import requests
from kafka import KafkaProducer

from betflow.api_connectors.conn_utils import RateLimiter
from betflow.api_connectors.raw_game_transformers import (
    api_raw_nba_data,
    api_raw_nfl_data,
    api_raw_nhl_data,
    api_raw_cfb_data,
)
from kafka.errors import NoBrokersAvailable
from datetime import datetime, timezone


class ESPNConnector:
    """Connector for ESPN API with Kafka integration.

    This class handles API requests to ESPN, data transformation, and publishing
    to Kafka topics with proper rate limiting and error handling.

    Args:
        kafka_bootstrap_servers (str): Comma-separated list of Kafka bootstrap servers.

    Attributes:
        base_url (str): Base URL for ESPN API endpoints.
        producer (KafkaProducer): Kafka producer instance.
        rate_limiter (RateLimiter): Rate limiter instance.
        session (requests.Session): Requests session for connection pooling.
    """

    def __init__(
        self, kafka_bootstrap_servers: Optional[str] = None, historical: bool = False
    ) -> None:
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports"
        # Add retry logic for Kafka connection
        max_retries = 3
        retry_count = 0

        if not historical:
            while retry_count < max_retries:
                try:
                    self.producer = KafkaProducer(
                        bootstrap_servers=kafka_bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        compression_type="gzip",
                        retries=3,
                        acks="all",
                        api_version=(2, 5, 0),  # Add explicit API version
                        security_protocol="PLAINTEXT",  # Specify security protocol
                        request_timeout_ms=30000,  # Increase timeout
                        connections_max_idle_ms=300000,  # Increase idle time
                    )
                    break
                except NoBrokersAvailable as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        raise Exception(
                            f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}"
                        )
                    time.sleep(3)  # Wait before retrying

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

        except requests.exceptions.HTTPError as e:
            if getattr(e.response, "status_code", None) == 429:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                raise Exception(
                    f"Rate limit exceeded. Retrying after {retry_after} seconds"
                )
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred: {e}")

    @staticmethod
    def _handle_request_error(error: Exception) -> None:
        """Handles various types of request errors.

        Args:
            error (Exception): The caught exception.

        Raises:
            Exception: With appropriate error message based on error type.
        """
        if isinstance(error, requests.exceptions.HTTPError):
            if error.response.status_code == 429:
                retry_after = int(error.response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                raise Exception(
                    f"Rate limit exceeded. Retrying after {retry_after} seconds"
                )
            elif error.response.status_code >= 500:
                raise Exception(f"ESPN API server error: {error}")
            else:
                raise Exception(f"HTTP error occurred: {error}")
        elif isinstance(error, requests.exceptions.Timeout):
            raise Exception("Request timed out")
        else:
            raise Exception(f"An error occurred: {error}")

    async def fetch_historical_games_by_date(
        self, sport: str, league: str, date_str: str
    ) -> List[Dict]:
        """Fetch historical games for a specific date"""
        endpoint = f"{sport}/{league}/scoreboard"
        params = {
            "dates": date_str.replace("-", "")  # Format: YYYYMMDD
        }

        try:
            url = f"{self.base_url}/{endpoint}"
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                # Process each game using existing transformers
                processed_games = []
                for game in data.get("events", []):
                    if league == "nba":
                        processed_game = self.api_raw_nba_data(game)
                    elif league == "nfl":
                        processed_game = self.api_raw_nfl_data(game)
                    elif league == "nhl":
                        processed_game = self.api_raw_nhl_data(game)
                    elif league == "college-football":
                        processed_game = self.api_raw_cfb_data(game)

                    if processed_game:
                        processed_games.append(processed_game)

                return processed_games

        except Exception as e:
            self._handle_request_error(e)

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publishes transformed data to Kafka topic.

        Args:
            topic (str): Kafka topic to publish to.
            data (Dict[str, Any]): Data to publish.

        Raises:
            Exception: If publishing fails.
        """
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Error publishing to Kafka: {e}")

    async def fetch_and_publish_games(
        self, sport: str, league: str, topic_name: str
    ) -> None:
        """Fetches games for a sport/league and publishes to Kafka.

        Args:
            sport (str): Sport name (e.g., 'basketball').
            league (str): League name (e.g., 'nba').
            topic_name (str): kafka topic name.

        Raises:
            Exception: If fetch and publish pipeline fails.
        """
        try:
            endpoint = f"{sport}/{league}/scoreboard"
            raw_data = self.make_request(endpoint)

            if not self.check_upcoming_games(raw_data):
                return False

            for game in raw_data.get("events", []):
                # TODO
                status = game.get("status", {}).get("type", {}).get("state")
                if status == "in":  # Skip completed games
                    if league == "nhl":
                        transformed_data = api_raw_nhl_data(game)
                    elif league == "nba":
                        transformed_data = api_raw_nba_data(game)
                    elif league == "nfl":
                        transformed_data = api_raw_nfl_data(game)
                    elif league == "college-football":
                        transformed_data = api_raw_cfb_data(game)
                    else:
                        transformed_data = None
                    self.publish_to_kafka(topic_name, transformed_data)
                    print(
                        f"Published {topic_name.split('.')[0]} game data for {game.get('name')}"
                    )
            return True
        # else:
        #     print(f"No games for {sport} with status='in'")

        except Exception as e:
            raise Exception(f"Error in fetch and publish pipeline: {e}")

    @staticmethod
    def check_upcoming_games(raw_data: dict, hours: int = 1) -> bool:
        """Check if there are any games starting within specified hours."""
        current_time = datetime.now(timezone.utc)
        for game in raw_data.get("events", []):
            game_time = datetime.fromisoformat(game.get("date").replace("Z", "+00:00"))
            time_until_game = (game_time - current_time).total_seconds() / 3600
            status = game.get("status", {}).get("type", {}).get("state")

            if status == "in" or (status == "pre" and time_until_game <= hours):
                return True
        return False

    def close(self) -> None:
        """Closes Kafka producer and cleans up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


# def main() -> None:
#     """Main function to demonstrate connector usage."""
#     connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")
#
#     try:
#         connector.fetch_and_publish_games("basketball", "nba")
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         connector.close()
#
#
# if __name__ == "__main__":
#     main()
