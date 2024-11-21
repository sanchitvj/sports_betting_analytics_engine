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


def get_live_games(espn_connector: ESPNConnector, sport: str, league: str) -> list:
    """Get live or upcoming NBA games."""
    try:
        endpoint = f"{sport}/{league}/scoreboard"
        raw_data = espn_connector.make_request(endpoint)

        games_list = []
        for game in raw_data.get("events", []):
            competition = game.get("competitions", [{}])[0]
            venue = competition.get("venue", {})

            # Get teams
            competitors = competition.get("competitors", [])
            home_team = next(
                (team for team in competitors if team.get("homeAway") == "home"), {}
            )
            away_team = next(
                (team for team in competitors if team.get("homeAway") == "away"), {}
            )

            game_info = {
                "game_id": game.get("id"),
                "home_team": home_team.get("team", {}).get("name"),
                "away_team": away_team.get("team", {}).get("name"),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("fullName"),
                "league": league,
                "status": game.get("status", {}).get("type", {}).get("state"),
                "start_time": game.get("date"),
            }
            games_list.append(game_info)

        return games_list
    except Exception as e:
        raise Exception(f"Failed to get live games: {e}")


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


async def run_basketball_pipeline(sport, league):
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

        logger.info(f"Found {len(games)} games:")
        for game in games:
            logger.info(
                f"{game['away_team']} @ {game['home_team']} - {game['venue_name']}"
            )

        # Start processor
        if league == "college-football":
            topic_league = "cfb"
        else:
            topic_league = league
        if topic_league == "cfb":
            game_processor = CFBProcessor(
                spark=spark,
                input_topic=f"{topic_league}.game.live",
                output_topic=f"{topic_league}_analytics",
                checkpoint_location="/tmp/checkpoint",
            )
        elif topic_league == "nba":
            game_processor = NBAProcessor(
                spark=spark,
                input_topic=f"{topic_league}.game.live",
                output_topic=f"{topic_league}_analytics",
                checkpoint_location="/tmp/checkpoint",
            )
        elif topic_league == "nfl":
            game_processor = NFLProcessor(
                spark=spark,
                input_topic=f"{topic_league}.game.live",
                output_topic=f"{topic_league}_analytics",
                checkpoint_location="/tmp/checkpoint",
            )
        else:
            game_processor = NHLProcessor(
                spark=spark,
                input_topic=f"{topic_league}.game.live",
                output_topic=f"{topic_league}_analytics",
                checkpoint_location="/tmp/checkpoint",
            )

        kafka_query = game_processor.process()
        logger.info("Started streaming query")

        # Continuous fetch loop
        while kafka_query.isActive:
            try:
                for game in games:
                    if game["status"] == "in":  # , "pre"]:  # Live or upcoming games
                        result = await espn_connector.fetch_and_publish_games(
                            sport=sport, league=league, topic_name=f"{league}.game.live"
                        )
                        logger.info(
                            f"Published game data for {game['away_team']} @ {game['home_team']}"
                        )

                        kafka_query.processAllAvailable()
                        logger.info("Processed available data")
                await asyncio.sleep(300)  # Update every 30 seconds

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


if __name__ == "__main__":
    try:
        shutil.rmtree("/tmp/checkpoint")
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")
    # espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")
    # games = get_live_games(espn_connector)
    asyncio.run(run_basketball_pipeline("basketball", "nba"))
