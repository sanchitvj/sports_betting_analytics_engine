import json
import time
from typing import Dict, Any, Optional
import requests
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter
from kafka.errors import NoBrokersAvailable

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

    def __init__(self, kafka_bootstrap_servers: str) -> None:
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports"
        # Add retry logic for Kafka connection
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    compression_type='gzip',
                    retries=3,
                    acks='all',
                    api_version=(2, 5, 0),  # Add explicit API version
                    security_protocol="PLAINTEXT",  # Specify security protocol
                    request_timeout_ms=30000,  # Increase timeout
                    connections_max_idle_ms=300000  # Increase idle time
                )
                break
            except NoBrokersAvailable as e:
                retry_count += 1
                if retry_count == max_retries:
                    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
                time.sleep(5)  # Wait before retrying

        self.rate_limiter = RateLimiter()
        self.session = requests.Session()

    def make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Makes a rate-limited request to the ESPN API."""
        self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if getattr(e.response, 'status_code', None) == 429:
                retry_after = int(e.response.headers.get('Retry-After', 60))
                raise Exception(f"Rate limit exceeded. Retrying after {retry_after} seconds")
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred: {e}")

    def _handle_request_error(self, error: Exception) -> None:
        """Handles various types of request errors.

        Args:
            error (Exception): The caught exception.

        Raises:
            Exception: With appropriate error message based on error type.
        """
        if isinstance(error, requests.exceptions.HTTPError):
            if error.response.status_code == 429:
                retry_after = int(error.response.headers.get('Retry-After', 60))
                time.sleep(retry_after)
                raise Exception(f"Rate limit exceeded. Retrying after {retry_after} seconds")
            elif error.response.status_code >= 500:
                raise Exception(f"ESPN API server error: {error}")
            else:
                raise Exception(f"HTTP error occurred: {error}")
        elif isinstance(error, requests.exceptions.Timeout):
            raise Exception("Request timed out")
        else:
            raise Exception(f"An error occurred: {error}")

    def transform_game_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms raw ESPN game data to defined schema format.

        Args:
            raw_data (Dict[str, Any]): Raw game data from ESPN API.

        Returns:
            Dict[str, Any]: Transformed game data.

        Raises:
            Exception: If transformation fails.
        """
        try:
            return {
                'game_id': raw_data.get('id'),
                'start_time': raw_data.get('date'),
                'status': raw_data.get('status', {}).get('type', {}).get('name'),
                'home_team': {
                    'id': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[0].get('id'),
                    'name': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[0].get('team', {}).get('name'),
                    'score': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[0].get('score')
                },
                'away_team': {
                    'id': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[1].get('id'),
                    'name': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[1].get('team', {}).get('name'),
                    'score': raw_data.get('competitions', [{}])[0].get('competitors', [{}])[1].get('score')
                },
                'venue': raw_data.get('competitions', [{}])[0].get('venue', {}).get('name'),
                'timestamp': int(time.time())
            }
        except Exception as e:
            raise Exception(f"Error transforming game data: {e}")

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

    def fetch_and_publish_games(self, sport: str, league: str) -> None:
        """Fetches games for a sport/league and publishes to Kafka.

        Args:
            sport (str): Sport name (e.g., 'basketball').
            league (str): League name (e.g., 'nba').

        Raises:
            Exception: If fetch and publish pipeline fails.
        """
        try:
            endpoint = f"{sport}/{league}/scoreboard"
            raw_data = self.make_request(endpoint)

            for game in raw_data.get('events', []):
                transformed_data = self.transform_game_data(game)
                self.publish_to_kafka(f"espn.{sport}.{league}.games", transformed_data)

        except Exception as e:
            raise Exception(f"Error in fetch and publish pipeline: {e}")

    def close(self) -> None:
        """Closes Kafka producer and cleans up resources."""
        self.producer.flush()
        self.producer.close()


def main() -> None:
    """Main function to demonstrate connector usage."""
    connector = ESPNConnector(kafka_bootstrap_servers='localhost:9092')

    try:
        connector.fetch_and_publish_games('basketball', 'nba')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        connector.close()

if __name__ == "__main__":
    main()