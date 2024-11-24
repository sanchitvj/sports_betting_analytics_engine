import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import ESPNConnector
import logging
import json
import shutil
from datetime import datetime
from betflow.spark_streaming.event_processor import (
    CFBProcessor,
    NBAProcessor,
    NFLProcessor,
    NHLProcessor,
)
from betflow.pipeline_utils import get_live_games


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def run_sports_pipeline(base_ckpt, sport, league):
    """Run the full basketball game streaming pipeline."""
    spark = (
        SparkSession.builder.appName("basketball_pipeline")
        .master("local[2]")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .getOrCreate()
    )

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Initialize connectors
        espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")

        # Get live/upcoming games
        games = get_live_games(espn_connector, sport, league)

        if not games:
            logger.info("No live or upcoming games found")
            return

        logger.info(f"Found {len(games)} {league} games:")
        for game in games:
            logger.info(
                f"{game['away_team']} @ {game['home_team']} - {game['venue_name']}"
            )
        processor_map = {
            "nba": NBAProcessor,
            "nfl": NFLProcessor,
            "nhl": NHLProcessor,
            "cfb": CFBProcessor,
        }

        topic_league = "cfb" if league == "college-football" else league
        processor_class = processor_map.get(topic_league)

        if not processor_class:
            raise ValueError(f"Unsupported league: {league}")

        game_processor = processor_class(
            spark=spark,
            input_topic=f"{topic_league}.game.live",
            output_topic=f"{topic_league}_games_analytics",
            checkpoint_location=f"{base_ckpt}/{topic_league}",
        )

        kafka_query = game_processor.process()
        logger.info("Started streaming query")

        # Continuous fetch loop
        while kafka_query.isActive:
            try:
                for game in games:
                    if game["status"] == "in":  # , "pre"]:  # Live or upcoming games
                        result = await espn_connector.fetch_and_publish_games(
                            sport=sport,
                            league=league,  # do not make it topic_league
                            topic_name=f"{topic_league}.game.live",
                        )
                        logger.info(
                            f"Published {topic_league} game data for {game['away_team']} @ {game['home_team']}"
                        )

                        kafka_query.processAllAvailable()
                        logger.info("Processed available data")
                await asyncio.sleep(180)  # Update every 30 seconds

            except Exception as e:
                logger.error(f"Error in fetch loop: {e}")
                await asyncio.sleep(60)

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        if "kafka_query" in locals() and kafka_query.isActive:
            kafka_query.stop()
        espn_connector.close()
        spark.stop()


async def main(base_ckpt):
    """Run all sports pipelines concurrently."""
    sports_config = [
        # ("basketball", "nba"),
        ("football", "nfl"),
        # ("football", "college-football"),
        # ("hockey", "nhl"),
    ]

    tasks = [
        run_sports_pipeline(base_ckpt, sport, league) for sport, league in sports_config
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    base_ckpt = "/tmp/checkpoint/games"
    try:
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    asyncio.run(main(base_ckpt))
