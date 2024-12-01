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


class GamesPipeline:
    def __init__(self):
        self.league_status = {"nba": True, "nhl": True, "nfl": True, "cfb": True}
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    async def run_sports_pipeline(self, base_ckpt, sport, league, spark=None):
        """Run the full basketball game streaming pipeline."""
        if not spark:
            spark = (
                SparkSession.builder.appName("games_pipeline")
                .master("local[4]")
                .config("spark.ui.port", "4040")
                .config("spark.driver.port", "50001")
                .config("spark.blockManager.port", "50002")
                .config("spark.driver.memory", "8g")
                .config("spark.sql.streaming.schemaInference", "true")
                .config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                    "org.apache.kafka:kafka-clients:3.5.1",
                )
                .getOrCreate()
            )

        try:
            espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")

            games = await get_live_games(espn_connector, sport, league)

            if not games:
                self.logger.info(f"No live or upcoming games found for {league}")
                return

            self.logger.info(f"Found {len(games)} {league} games:")
            for game in games:
                self.logger.info(
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
            self.logger.info("Started streaming query")

            # Continuous fetch loop
            while kafka_query.isActive:
                try:
                    has_games = await espn_connector.fetch_and_publish_games(
                        sport=sport,
                        league=league,  # do not make it topic_league
                        topic_name=f"{topic_league}.game.live",
                    )

                    if not has_games:
                        self.league_status[topic_league] = False
                        self.logger.info(
                            f"No games in next 1 hour for {topic_league.upper()}. Games pipeline may stop."
                        )
                        if not any(self.league_status.values()):
                            print("\n" + "=" * 60 + "\n")
                            self.logger.info(
                                "No games for any sport in next 1 hour. STOPPING PIPELINE"
                            )
                            print("\n" + "=" * 60)
                            break
                    else:
                        self.logger.info(
                            f"Games for {topic_league} on going or upcoming in 1 hour."
                        )
                        self.league_status[topic_league] = True

                    kafka_query.processAllAvailable()
                    await asyncio.sleep(60)

                except Exception as e:
                    self.logger.error(f"Error in fetch loop: {e}")
                    await asyncio.sleep(60)

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise
        finally:
            if "kafka_query" in locals() and kafka_query.isActive:
                kafka_query.stop()
            espn_connector.close()
            spark.stop()


async def main(base_ckpt):
    """Run all sports pipelines concurrently."""
    pipeline = GamesPipeline()
    sports_config = [
        ("basketball", "nba"),
        ("football", "nfl"),
        ("football", "college-football"),
        ("hockey", "nhl"),
    ]

    tasks = [
        pipeline.run_sports_pipeline(base_ckpt, sport, league)
        for sport, league in sports_config
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    base_ckpt = "/tmp/checkpoint/games"
    try:
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    asyncio.run(main(base_ckpt))
