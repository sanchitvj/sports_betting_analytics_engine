import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import OpenWeatherConnector
from betflow.spark_streaming.event_processor import WeatherProcessor
import logging
import json
import shutil
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def run_weather_pipeline():
    """Run the full weather data pipeline."""

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("weather_pipeline_test")
        .master("local[2]")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .getOrCreate()
    )

    # Add logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Initialize components
        weather_connector = OpenWeatherConnector(
            api_key="",  # os.getenv("OPENWEATHER_API_KEY"),
            kafka_bootstrap_servers="localhost:9092",
        )

        # Test venues
        venues = [
            {
                "city": "DC",
                "venue_id": "venue_dc",
                "game_id": "game123",
                "lat": 38.9072,
                "lon": -77.0369,
            }
        ]

        logger.info("Starting weather processor...")
        weather_processor = WeatherProcessor(
            spark=spark,
            input_topic="weather.current.dc",
            output_topic="weather_analytics",
            checkpoint_location="/tmp/checkpoint",
        )

        kafka_query = weather_processor.process()
        logger.info("Started streaming query")

        # Continuous fetch loop
        while kafka_query.isActive:
            try:
                for venue in venues:
                    result = await weather_connector.fetch_and_publish_weather(
                        city=venue["city"],
                        venue_id=venue["venue_id"],
                        game_id=venue["game_id"],
                        lat=venue["lat"],
                        lon=venue["lon"],
                    )
                    logger.info(f"Published weather data for {venue['city']}")

                kafka_query.processAllAvailable()

                logger.info("Processed available data")
                await asyncio.sleep(300)  # 5 minutes

            except Exception as e:
                logger.error(f"Error in fetch loop: {e}")
                await asyncio.sleep(60)

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise
    finally:
        if "kafka_query" in locals() and kafka_query.isActive:
            kafka_query.stop()
        weather_connector.close()
        spark.stop()


if __name__ == "__main__":
    try:
        shutil.rmtree("/tmp/checkpoint")
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")
    asyncio.run(run_weather_pipeline())
