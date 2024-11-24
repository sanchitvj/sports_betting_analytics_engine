import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import OpenWeatherConnector, ESPNConnector
from betflow.spark_streaming.event_processor import WeatherProcessor
import logging
import shutil
from datetime import datetime, timedelta
import requests
from betflow.pipeline_utils import get_live_games


class WeatherPipeline:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("weather_pipeline")
            .master("local[2]")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .getOrCreate()
        )

        self.logger = logging.getLogger(__name__)
        self.weather_connector = OpenWeatherConnector(
            api_key="your_key", kafka_bootstrap_servers="localhost:9092"
        )
        self.espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")

    async def get_outdoor_venues(self):
        outdoor_venues = []
        sports = [("football", "nfl"), ("football", "college-football")]

        for sport, league in sports:
            games = get_live_games(self.espn_connector, sport, league)
            for game in games:
                if game.get("status") in ["pre", "in"] and not game.get(
                    "venue", {}
                ).get("indoor", True):
                    venue = {
                        "city": game["venue_city"],
                        "state": game["venue_state"],
                        "venue_id": game["venue_id"],
                        "game_id": game["game_id"],
                        "start_time": datetime.fromisoformat(game["start_time"]),
                        "status": game["status"],
                        "league": league,  # Add league information
                    }
                    coords = self.get_coordinates(venue["city"], venue["state"])
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
                "appid": "your_openweather_key",
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
        now = datetime.now()
        start_time = venue["start_time"]

        # Stream if:
        # 1. Game starts within 3 hours
        # 2. Game is in progress
        # 3. Game started less than 4 hours ago (typical game duration)
        if venue["status"] == "pre":
            return (start_time - now) <= timedelta(hours=3)
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

                    for venue in venues:
                        if await self.should_stream_weather(venue):
                            league = venue.get("league")
                            topic_league = (
                                "cfb" if league == "college-football" else league
                            )
                            await self.weather_connector.fetch_and_publish_weather(
                                topic_name=f"{topic_league}.weather.current",
                                city=venue["city"],
                                venue_id=venue["venue_id"],
                                game_id=venue["game_id"],
                                lat=venue["lat"],
                                lon=venue["lon"],
                            )
                            self.logger.info(
                                f"Published weather data for {venue['city']} ({league})"
                            )

                    for query in queries.values():
                        query.processAllAvailable()
                    await asyncio.sleep(300)

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
    try:
        base_ckpt = "/tmp/checkpoint/weather"
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    pipeline = WeatherPipeline()
    asyncio.run(pipeline.run(base_ckpt))
