import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import OddsAPIConnector
import logging
import shutil
from betflow.spark_streaming.event_processor import OddsProcessor
from betflow.pipeline_utils import get_live_odds
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv()


def calculate_pipeline_timing(odds_data: list) -> tuple:
    """Calculate pipeline timing based on closest game start time."""
    if not odds_data:
        return 1800, 35  # Default 30 mins, 35 mins window

    current_time = datetime.now(timezone.utc)
    closest_game_time = min(
        datetime.fromisoformat(game["commence_time"].replace("Z", "+00:00"))
        for game in odds_data
    )

    remaining_hours = (closest_game_time - current_time).total_seconds() / 3600

    if any(game.get("status") == "in" for game in odds_data):
        return 600, 15  # 10 mins, 15 mins window for live games
    elif remaining_hours >= 4:
        return 1800, 35  # 30 mins, 35 mins window
    elif remaining_hours >= 3:
        return 1500, 30  # 25 mins, 30 mins window
    elif remaining_hours >= 2:
        return 1200, 25  # 20 mins, 25 mins window
    elif remaining_hours >= 1:
        return 900, 20  # 15 mins, 20 mins window
    else:
        return 600, 15  # 10 mins, 15 mins window


class OddsPipeline:
    def __init__(self):
        self.league_status = {
            "basketball_nba": True,
            "icehockey_nhl": True,
            "americanfootball_nfl": True,
            "americanfootball_ncaaf": True,
        }
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    async def run_odds_pipeline(self, base_ckpt: str, sport: str):
        """Run the full odds streaming pipeline."""
        spark = (
            SparkSession.builder.appName("odds_pipeline")
            .master("local[4]")
            .config("spark.ui.port", "4041")
            .config("spark.driver.port", "50003")
            .config("spark.blockManager.port", "50004")
            .config("spark.driver.memory", "8g")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.apache.kafka:kafka-clients:3.5.1,",
            )
            .getOrCreate()
        )

        try:
            # Initialize connector
            odds_connector = OddsAPIConnector(
                api_key=os.getenv("ODDS_API_KEY"),
                kafka_bootstrap_servers="localhost:9092",
            )

            # Get initial odds data
            odds_data = get_live_odds(odds_connector, sport)
            if not odds_data:
                self.logger.info("No odds data found")
                return

            self.logger.info(f"Found odds for {len(odds_data)} games")

            # Start processor
            sport_code = sport.split("_")[1] if "_" in sport else sport
            sport_code = "cfb" if sport_code == "ncaaf" else sport_code
            pipeline_time, window_time = calculate_pipeline_timing(odds_data)
            print(
                f"[Processor] pipeline sleep time @ {pipeline_time} and window @ {window_time}"
            )
            odds_processor = OddsProcessor(
                spark=spark,
                input_topic=f"{sport_code}.odds.live",
                output_topic=f"{sport_code}_odds_analytics",
                checkpoint_location=f"{base_ckpt}/{sport_code}",
                window_duration=window_time,
            )

            kafka_query = odds_processor.process()
            self.logger.info("Started odds streaming query")

            # Continuous fetch loop
            while kafka_query.isActive:
                try:
                    if odds_data:
                        has_games = await odds_connector.fetch_and_publish_odds(
                            sport=sport, topic_name=f"{sport_code}.odds.live"
                        )

                        if not has_games:
                            self.league_status[sport] = False
                            self.logger.info(
                                f"No games in next 4 hours for {sport}. Odds pipeline may stop."
                            )
                            if not any(self.league_status.values()):
                                print("\n" + "=" * 60 + "\n")
                                self.logger.info(
                                    "No games for any sport in next 4 hour. STOPPING PIPELINE"
                                )
                                print("\n" + "=" * 60)
                                break
                        else:
                            self.league_status[sport] = True

                    kafka_query.processAllAvailable()
                    pipeline_time, _ = calculate_pipeline_timing(odds_data)
                    print(
                        f"[Connector] pipeline sleep time @ {pipeline_time} and window @ {window_time}"
                    )
                    await asyncio.sleep(pipeline_time)

                except Exception as e:
                    self.logger.error(f"Error in odds fetch loop: {e}")
                    await asyncio.sleep(180)

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise
        finally:
            if "kafka_query" in locals() and kafka_query.isActive:
                kafka_query.stop()
            odds_connector.close()
            spark.stop()


async def main(base_ckpt):
    """Run odds pipelines for multiple sports concurrently."""
    pipeline = OddsPipeline()
    sports_config = [
        "basketball_nba",
        "icehockey_nhl",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
    ]

    tasks = [pipeline.run_odds_pipeline(base_ckpt, sport) for sport in sports_config]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    base_ckpt = "/tmp/checkpoint/odds"
    try:
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    asyncio.run(main(base_ckpt))
