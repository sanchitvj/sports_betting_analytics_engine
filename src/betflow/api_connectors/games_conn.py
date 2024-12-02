import json
import time
from typing import Dict, Any, Optional, List
import requests
from kafka import KafkaProducer

from betflow.api_connectors.conn_utils import RateLimiter
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

    @staticmethod
    def api_raw_cfb_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            competition = raw_data.get("competitions", [{}])[0]
            home_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "home"
                ),
                {},
            )
            away_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "away"
                ),
                {},
            )

            def get_leader_value(competition, category):
                leaders = competition.get("leaders", [])
                leader = next((l for l in leaders if l.get("name") == category), {})
                leader_stats = (
                    leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
                )
                return {
                    "displayValue": leader_stats.get("displayValue"),
                    "value": leader_stats.get("value"),
                    "athlete": leader_stats.get("athlete", {}).get("displayName"),
                    "team": leader_stats.get("team", {}).get("id"),
                }

            cfb_game_data = {
                "game_id": raw_data.get("id"),
                "start_time": raw_data.get("date"),
                "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
                "status_detail": raw_data.get("status", {})
                .get("type", {})
                .get("detail"),
                "status_description": raw_data.get("status", {})
                .get("type", {})
                .get("description"),
                "period": raw_data.get("status", {}).get("period", 0),
                "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
                # Home team
                "home_team_name": home_team.get("team", {}).get("name"),
                "home_team_id": home_team.get("team", {}).get("id"),
                "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
                "home_team_score": int(home_team.get("score", 0)),
                "home_team_record": next(
                    (
                        r.get("summary")
                        for r in home_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                "home_team_linescores": [
                    int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
                ],
                # Away team
                "away_team_name": away_team.get("team", {}).get("name"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
                "away_team_score": int(away_team.get("score", 0)),
                "away_team_record": next(
                    (
                        r.get("summary")
                        for r in away_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                "away_team_linescores": [
                    int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
                ],
                # Game Leaders
                "passing_leader_name": get_leader_value(
                    competition, "passingYards"
                ).get("athlete"),
                "passing_leader_display_value": get_leader_value(
                    competition, "passingYards"
                ).get("displayValue"),
                "passing_leader_value": get_leader_value(
                    competition, "passingYards"
                ).get("value"),
                "passing_leader_team": get_leader_value(
                    competition, "passingYards"
                ).get("team"),
                "rushing_leader_name": get_leader_value(
                    competition, "rushingYards"
                ).get("athlete"),
                "rushing_leader_display_value": get_leader_value(
                    competition, "rushingYards"
                ).get("displayValue"),
                "rushing_leader_value": get_leader_value(
                    competition, "rushingYards"
                ).get("value"),
                "rushing_leader_team": get_leader_value(
                    competition, "rushingYards"
                ).get("team"),
                "receiving_leader_name": get_leader_value(
                    competition, "receivingYards"
                ).get("athlete"),
                "receiving_leader_display_value": get_leader_value(
                    competition, "receivingYards"
                ).get("displayValue"),
                "receiving_leader_value": get_leader_value(
                    competition, "receivingYards"
                ).get("value"),
                "receiving_leader_team": get_leader_value(
                    competition, "receivingYards"
                ).get("team"),
                # Venue
                "venue_name": competition.get("venue", {}).get("fullName"),
                "venue_city": competition.get("venue", {})
                .get("address", {})
                .get("city"),
                "venue_state": competition.get("venue", {})
                .get("address", {})
                .get("state"),
                "venue_indoor": competition.get("venue", {}).get("indoor", False),
                "broadcasts": [
                    broadcast.get("names", [])[0]
                    for broadcast in competition.get("broadcasts", [])
                ],
                "timestamp": int(time.time()),
            }
            return cfb_game_data  # CFBGameStats(**cfb_game_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform college football data: {e}")

    @staticmethod
    def api_raw_nfl_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            competition = raw_data.get("competitions", [{}])[0]
            home_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "home"
                ),
                {},
            )
            away_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "away"
                ),
                {},
            )

            # Leaders are at competition level when game is live
            def get_leader_value(competition, category):
                leaders = competition.get("leaders", [])
                leader = next((l for l in leaders if l.get("name") == category), {})
                leader_stats = (
                    leader.get("leaders", [{}])[0] if leader.get("leaders") else {}
                )
                return {
                    "displayValue": leader_stats.get("displayValue"),
                    "value": leader_stats.get("value"),
                    "athlete": leader_stats.get("athlete", {}).get("displayName"),
                    "team": leader_stats.get("team", {}).get("id"),
                }

            nfl_games_data = {
                "game_id": raw_data.get("id"),
                "start_time": raw_data.get("date"),
                "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
                "status_detail": raw_data.get("status", {})
                .get("type", {})
                .get("detail"),
                "status_description": raw_data.get("status", {})
                .get("type", {})
                .get("description"),
                "period": raw_data.get("status", {}).get("period", 0),
                "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
                # Home team
                "home_team_name": home_team.get("team", {}).get("name"),
                "home_team_id": home_team.get("team", {}).get("id"),
                "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
                "home_team_score": int(home_team.get("score", 0)),
                "home_team_record": next(
                    (
                        r.get("summary")
                        for r in home_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                "home_team_linescores": [
                    int(ls.get("value", 0)) for ls in home_team.get("linescores", [])
                ],
                # Away team
                "away_team_name": away_team.get("team", {}).get("name"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
                "away_team_score": int(away_team.get("score", 0)),
                "away_team_record": next(
                    (
                        r.get("summary")
                        for r in away_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                "away_team_linescores": [
                    int(ls.get("value", 0)) for ls in away_team.get("linescores", [])
                ],
                # Game Leaders
                "passing_leader_name": get_leader_value(
                    competition, "passingYards"
                ).get("athlete"),
                "passing_leader_display_value": get_leader_value(
                    competition, "passingYards"
                ).get("displayValue"),
                "passing_leader_value": get_leader_value(
                    competition, "passingYards"
                ).get("value"),
                "passing_leader_team": get_leader_value(
                    competition, "passingYards"
                ).get("team"),
                "rushing_leader_name": get_leader_value(
                    competition, "rushingYards"
                ).get("athlete"),
                "rushing_leader_display_value": get_leader_value(
                    competition, "rushingYards"
                ).get("displayValue"),
                "rushing_leader_value": get_leader_value(
                    competition, "rushingYards"
                ).get("value"),
                "rushing_leader_team": get_leader_value(
                    competition, "rushingYards"
                ).get("team"),
                "receiving_leader_name": get_leader_value(
                    competition, "receivingYards"
                ).get("athlete"),
                "receiving_leader_display_value": get_leader_value(
                    competition, "receivingYards"
                ).get("displayValue"),
                "receiving_leader_value": get_leader_value(
                    competition, "receivingYards"
                ).get("value"),
                "receiving_leader_team": get_leader_value(
                    competition, "receivingYards"
                ).get("team"),
                # Venue
                "venue_name": competition.get("venue", {}).get("fullName"),
                "venue_city": competition.get("venue", {})
                .get("address", {})
                .get("city"),
                "venue_state": competition.get("venue", {})
                .get("address", {})
                .get("state"),
                "venue_indoor": competition.get("venue", {}).get("indoor", False),
                "broadcasts": [
                    broadcast.get("names", [])[0]
                    for broadcast in competition.get("broadcasts", [])
                ],
                "timestamp": int(time.time()),
            }
            return nfl_games_data  # NFLGameStats(**nfl_games_data).model_dump()
        except Exception as e:
            raise Exception(f"Error transforming NFL game data: {e}")

    @staticmethod
    def api_raw_nhl_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms raw ESPN NHL game data to defined schema format."""
        try:
            competition = raw_data.get("competitions", [{}])[0]
            home_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "home"
                ),
                {},
            )
            away_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "away"
                ),
                {},
            )

            # Get team statistics
            home_stats = home_team.get("statistics", [])
            away_stats = away_team.get("statistics", [])

            # Helper function to get stat value
            def get_stat_value(stats, name):
                stat = next((s for s in stats if s.get("name") == name), {})
                return stat.get("displayValue")

            nhl_game_data = {
                "game_id": raw_data.get("id"),
                "start_time": raw_data.get("date"),
                # Game status
                "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
                "status_detail": raw_data.get("status", {})
                .get("type", {})
                .get("detail"),
                "status_description": raw_data.get("status", {})
                .get("type", {})
                .get("description"),
                "period": raw_data.get("status", {}).get("period", 0),
                "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
                # Home team
                "home_team_name": home_team.get("team", {}).get("name"),
                "home_team_id": home_team.get("team", {}).get("id"),
                "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
                "home_team_score": home_team.get("score"),
                # Home team statistics
                "home_team_saves": get_stat_value(home_stats, "saves"),
                "home_team_save_pct": get_stat_value(home_stats, "savePct"),
                "home_team_goals": get_stat_value(home_stats, "goals"),
                "home_team_assists": get_stat_value(home_stats, "assists"),
                "home_team_points": get_stat_value(home_stats, "points"),
                "home_team_penalties": get_stat_value(home_stats, "penalties"),
                "home_team_penalty_minutes": get_stat_value(
                    home_stats, "penaltyMinutes"
                ),
                "home_team_power_plays": get_stat_value(home_stats, "powerPlays"),
                "home_team_power_play_goals": get_stat_value(
                    home_stats, "powerPlayGoals"
                ),
                "home_team_power_play_pct": get_stat_value(home_stats, "powerPlayPct"),
                # Away team
                "away_team_name": away_team.get("team", {}).get("name"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
                "away_team_score": away_team.get("score"),
                # Away team statistics
                "away_team_saves": get_stat_value(away_stats, "saves"),
                "away_team_save_pct": get_stat_value(away_stats, "savePct"),
                "away_team_goals": get_stat_value(away_stats, "goals"),
                "away_team_assists": get_stat_value(away_stats, "assists"),
                "away_team_points": get_stat_value(away_stats, "points"),
                "away_team_penalties": get_stat_value(away_stats, "penalties"),
                "away_team_penalty_minutes": get_stat_value(
                    away_stats, "penaltyMinutes"
                ),
                "away_team_power_plays": get_stat_value(away_stats, "powerPlays"),
                "away_team_power_play_goals": get_stat_value(
                    away_stats, "powerPlayGoals"
                ),
                "away_team_power_play_pct": get_stat_value(away_stats, "powerPlayPct"),
                # Team records
                "home_team_record": next(
                    (
                        r.get("summary")
                        for r in home_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                "away_team_record": next(
                    (
                        r.get("summary")
                        for r in away_team.get("records", [])
                        if r.get("name") == "overall"
                    ),
                    "0-0",
                ),
                # Venue information
                "venue_name": competition.get("venue", {}).get("fullName"),
                "venue_city": competition.get("venue", {})
                .get("address", {})
                .get("city"),
                "venue_state": competition.get("venue", {})
                .get("address", {})
                .get("state"),
                "venue_indoor": competition.get("venue", {}).get("indoor", True),
                # Broadcasts and timestamp
                "broadcasts": [
                    broadcast.get("names", [])[0]
                    for broadcast in competition.get("broadcasts", [])
                ],
                "timestamp": int(time.time()),
            }
            return nhl_game_data  # NHLGameStats(**nhl_game_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform NHL game data: {e}")

    @staticmethod
    def api_raw_nba_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms raw ESPN game data to defined schema format."""
        try:
            competition = raw_data.get("competitions", [{}])[0]
            home_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "home"
                ),
                {},
            )
            away_team = next(
                (
                    team
                    for team in competition.get("competitors", [])
                    if team.get("homeAway") == "away"
                ),
                {},
            )

            # Get team statistics
            home_stats = home_team.get("statistics", [])
            away_stats = away_team.get("statistics", [])

            # Helper function to get stat value
            def get_stat_value(stats, name):
                stat = next((s for s in stats if s.get("name") == name), {})
                return stat.get("displayValue")

            nba_game_data = {
                "game_id": raw_data.get("id"),
                "start_time": raw_data.get("date"),
                # game status
                "status_state": raw_data.get("status", {}).get("type", {}).get("state"),
                "status_detail": raw_data.get("status", {})
                .get("type", {})
                .get("detail"),
                "status_description": raw_data.get("status", {})
                .get("type", {})
                .get("description"),
                "period": raw_data.get("status", {}).get("period", 0),
                "clock": raw_data.get("status", {}).get("displayClock", "0:00"),
                # home team
                "home_team_name": home_team.get("team", {}).get("name"),
                "home_team_id": home_team.get("team", {}).get("id"),
                "home_team_abbreviation": home_team.get("team", {}).get("abbreviation"),
                "home_team_score": home_team.get("score"),
                # Home team statistics
                "home_team_field_goals": get_stat_value(home_stats, "fieldGoalPct"),
                "home_team_three_pointers": get_stat_value(home_stats, "threePointPct"),
                "home_team_free_throws": get_stat_value(home_stats, "freeThrowPct"),
                "home_team_rebounds": get_stat_value(home_stats, "rebounds"),
                "home_team_assists": get_stat_value(home_stats, "assists"),
                # away team
                "away_team_name": away_team.get("team", {}).get("name"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "away_team_abbreviation": away_team.get("team", {}).get("abbreviation"),
                "away_team_score": away_team.get("score"),
                # Away team statistics
                "away_team_field_goals": get_stat_value(away_stats, "fieldGoalPct"),
                "away_team_three_pointers": get_stat_value(away_stats, "threePointPct"),
                "away_team_free_throws": get_stat_value(away_stats, "freeThrowPct"),
                "away_team_rebounds": get_stat_value(away_stats, "rebounds"),
                "away_team_assists": get_stat_value(away_stats, "assists"),
                # venue
                "venue_name": competition.get("venue", {}).get("fullName"),
                "venue_city": competition.get("venue", {})
                .get("address", {})
                .get("city"),
                "venue_state": competition.get("venue", {})
                .get("address", {})
                .get("state"),
                "broadcasts": [
                    broadcast.get("names", [])[0]
                    for broadcast in competition.get("broadcasts", [])
                ],
                "timestamp": int(time.time()),
            }
            return nba_game_data  # NBAGameStats(**nba_game_data).model_dump()

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
                        transformed_data = self.api_raw_nhl_data(game)
                    elif league == "nba":
                        transformed_data = self.api_raw_nba_data(game)
                    elif league == "nfl":
                        transformed_data = self.api_raw_nfl_data(game)
                    elif league == "college-football":
                        transformed_data = self.api_raw_cfb_data(game)
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
