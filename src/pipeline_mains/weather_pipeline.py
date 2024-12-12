import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import OpenWeatherConnector, ESPNConnector
from betflow.spark_streaming.event_processor import WeatherProcessor
import logging
import shutil
from datetime import datetime, timedelta, timezone
import requests
from betflow.pipeline_utils import get_live_games
from dotenv import load_dotenv
import os

load_dotenv("my.env")


class WeatherPipeline:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("weather_pipeline")
            .master("local[2]")
            .config("spark.ui.port", "4042")
            .config("spark.driver.port", "50005")
            .config("spark.blockManager.port", "50006")
            .config("spark.driver.memory", "8g")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.apache.kafka:kafka-clients:3.5.1",
            )
            .getOrCreate()
        )
        self.league_status = {"nfl": True, "cfb": True}
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.weather_connector = OpenWeatherConnector(
            api_key=os.getenv("OPEN_WEATHER_API_KEY"),
            kafka_bootstrap_servers="localhost:9092",
        )
        self.espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")

    async def get_outdoor_venues(self):
        outdoor_venues = []
        sports = [("football", "nfl"), ("football", "college-football")]

        for sport, league in sports:
            games = await get_live_games(self.espn_connector, sport, league)
            for game in games:
                if game.get("status") in ["pre", "in"] and not game.get(
                    "venue_indoor", True
                ):
                    venue = {
                        "city": game["venue_city"],
                        "state_code": game["venue_state"],
                        "venue_id": game["venue_id"],
                        "game_id": game["game_id"],
                        "game_name": f"{game['home_team']} vs {game['away_team']}",
                        "start_time": datetime.fromisoformat(game["start_time"]),
                        "status": game["status"],
                        "league": league,  # Add league information
                    }
                    print(game.get("venue_indoor"))
                    coords = self.get_coordinates(venue["city"], venue["state_code"])
                    if coords:
                        venue.update(coords)
                        outdoor_venues.append(venue)

        return outdoor_venues

    def get_coordinates(self, city: str, state: str) -> dict:
        """Get coordinates using OpenWeather's geocoding API."""
        try:
            url = "http://api.openweathermap.org/geo/1.0/direct"
            params = {
                "q": f"{city},{state},US",
                "limit": 1,
                "appid": os.getenv("OPEN_WEATHER_API_KEY"),
            }
            response = requests.get(url, params=params)
            data = response.json()
            if data:
                return {"lat": data[0]["lat"], "lon": data[0]["lon"]}
        except Exception as e:
            self.logger.error(f"Error getting coordinates: {e}")
        return None

    @staticmethod
    async def should_stream_weather(venue: dict) -> bool:
        """Determine if weather should be streamed for this venue."""
        now = datetime.now(timezone.utc)  # Make current time timezone-aware

        # Ensure start_time is timezone-aware
        if isinstance(venue["start_time"], str):
            start_time = datetime.fromisoformat(
                venue["start_time"].replace("Z", "+00:00")
            )
        else:
            start_time = venue["start_time"]

        # Stream if:
        # 1. Game starts within 6 hours
        # 2. Game is in progress
        # 3. Game started less than 4 hours ago
        if venue["status"] == "pre":
            return (start_time - now) <= timedelta(hours=4)
        elif venue["status"] == "in":
            return (now - start_time) <= timedelta(hours=4)
        return False

    async def run(self, base_ckpt):
        try:
            # Map of leagues and their processors
            processors = {}
            queries = {}

            sports_config = [("football", "nfl"), ("football", "college-football")]

            # Initialize processors for each league
            for sport, league in sports_config:
                topic_league = "cfb" if league == "college-football" else league
                processors[league] = WeatherProcessor(
                    spark=self.spark,
                    input_topic=f"{topic_league}.weather.current",
                    output_topic=f"{topic_league}_weather_analytics",
                    checkpoint_location=f"{base_ckpt}/{topic_league}",
                )
                queries[league] = processors[league].process()
                self.logger.info(f"Started weather streaming query for {league}")

            while any(query.isActive for query in queries.values()):
                try:
                    venues = await self.get_outdoor_venues()
                    active_leagues = set()

                    for venue in venues:
                        if await self.should_stream_weather(venue):
                            league = venue.get("league")
                            topic_league = (
                                "cfb" if league == "college-football" else league
                            )
                            active_leagues.add(topic_league)
                            await self.weather_connector.fetch_and_publish_weather(
                                topic_name=f"{topic_league}.weather.current",
                                venue=venue,
                            )
                        else:
                            self.logger.info("No weather data to publish.")

                    # Update status for all leagues
                    for league in self.league_status:
                        self.league_status[league] = league in active_leagues
                        if not self.league_status[league]:
                            self.logger.info(
                                f"No outdoor games in next 3 hours for {league.upper()}. Weather pipeline may stop."
                            )

                    # Check if all leagues are inactive
                    if not any(self.league_status.values()):
                        print("\n" + "=" * 60 + "\n")
                        self.logger.info(
                            "No outdoor games for any sport in next 3 hours. STOPPING PIPELINE"
                        )
                        print("\n" + "=" * 60)
                        break

                    for query in queries.values():
                        query.processAllAvailable()
                    await asyncio.sleep(900)

                except Exception as e:
                    self.logger.error(f"Error in fetch loop: {e}")
                    await asyncio.sleep(60)

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise
        finally:
            for query in queries.values():
                if query.isActive:
                    query.stop()
            self.weather_connector.close()
            self.spark.stop()


if __name__ == "__main__":
    base_ckpt = "/tmp/checkpoint/weather"
    try:
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    pipeline = WeatherPipeline()
    asyncio.run(pipeline.run(base_ckpt))
