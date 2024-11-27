import json
import time
from typing import Dict, Any, Optional
import requests
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter
from datetime import datetime, timedelta, timezone


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

        if params is None:
            params = {}
        params["apiKey"] = self.api_key
        params["regions"] = "us"
        params["markets"] = "h2h"
        # headers = {"apikey": self.api_key}
        # print(url)
        try:
            response = self.session.get(url, params=params, timeout=30)
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
    def api_raw_odds_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw odds data with pre-calculated analytics.

        Args:
            raw_data: Raw data from API

        Returns:
            Transformed data with odds analytics
        """
        # Extract basic game info
        game_info = {
            "game_id": raw_data.get("id"),
            "sport_key": raw_data.get("sport_key"),
            "sport_title": raw_data.get("sport_title"),
            "commence_time": raw_data.get("commence_time"),
            "home_team": raw_data.get("home_team"),
            "away_team": raw_data.get("away_team"),
        }

        # Process bookmaker odds
        home_odds = []
        away_odds = []
        last_updates = []

        for bm in raw_data.get("bookmakers", []):
            bookie_key = bm.get("key")
            last_updates.append(bm.get("last_update"))

            for market in bm.get("markets", []):
                if market.get("key") == "h2h":
                    for outcome in market.get("outcomes", []):
                        if outcome.get("name") == game_info["home_team"]:
                            home_odds.append(
                                {
                                    "bookie_key": bookie_key,
                                    "price": outcome.get("price"),
                                }
                            )
                        elif outcome.get("name") == game_info["away_team"]:
                            away_odds.append(
                                {
                                    "bookie_key": bookie_key,
                                    "price": outcome.get("price"),
                                }
                            )

        # Calculate odds analytics
        home_prices = [odd["price"] for odd in home_odds]
        away_prices = [odd["price"] for odd in away_odds]

        return {
            **game_info,
            "home_odds_by_bookie": home_odds,
            "away_odds_by_bookie": away_odds,
            "best_home_odds": max(home_prices) if home_prices else None,
            "best_away_odds": max(away_prices) if away_prices else None,
            # "min_home_odds": min(home_prices) if home_prices else None,
            # "min_away_odds": min(away_prices) if away_prices else None,
            "avg_home_odds": sum(home_prices) / len(home_prices)
            if home_prices
            else None,
            "avg_away_odds": sum(away_prices) / len(away_prices)
            if away_prices
            else None,
            "bookmakers_count": len(raw_data.get("bookmakers", [])),
            "last_update": max(last_updates) if last_updates else None,
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
                "apiKey": self.api_key,
                "regions": regions,
                "markets": markets,
                "oddsFormat": odds_format,
            }
            # f"sports/{sport}/odds/?apikey={self.api_key}&regions={regions}&markets={markets}"
            raw_data = self.make_request(
                f"sports/{sport}/odds/",
                params=params,
            )

            current_time = datetime.now(timezone.utc)

            for game_odds in raw_data:
                # Convert commence_time to datetime
                commence_time = datetime.fromisoformat(
                    game_odds.get("commence_time").replace("Z", "+00:00")
                )

                # Calculate time until game starts
                time_until_game = commence_time - current_time

                # Only publish if:
                # 1. Game starts within next 4 hours OR
                # 2. Game has already started but not finished
                if (
                    timedelta(hours=-4) <= time_until_game <= timedelta(hours=4)
                    or game_odds.get("status") == "in"
                ):
                    transformed_data = self.api_raw_odds_data(game_odds)
                    self.publish_to_kafka(topic_name, transformed_data)
                    print(
                        f"Published odds data for {game_odds['sport_title']}: {game_odds['home_team']} vs {game_odds['away_team']}"
                    )

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
