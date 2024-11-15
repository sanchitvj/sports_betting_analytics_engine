# run_game_pipeline.py
import asyncio
from datetime import datetime
from pyspark.sql import SparkSession
from betflow.api_connectors import ESPNConnector
from betflow.spark_streaming.event_processor import GamesProcessor


async def run_game_pipeline():
    """Run the full game data pipeline."""

    # Initialize Spark with Kafka package
    spark = (
        SparkSession.builder.appName("game_pipeline_test")
        .master("local[2]")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .getOrCreate()
    )

    # Initialize components
    espn_connector = ESPNConnector(kafka_bootstrap_servers="localhost:9092")

    games_processor = GamesProcessor(
        spark=spark,
        input_topic="espn.basketball.nba.games",
        output_topic="game_analytics",
        checkpoint_location="/tmp/checkpoint",
    )

    try:
        # Start the streaming query
        query = games_processor.process()

        print("Started streaming query...")

        # Continuous data fetching loop
        while query.isActive:
            try:
                # Fetch and publish NBA games
                await espn_connector.fetch_and_publish_games("basketball", "nba")
                print(
                    f"[{datetime.now()}] Successfully fetched and published NBA games"
                )

                # Process available data
                query.processAllAvailable()

                # Wait before next fetch
                await asyncio.sleep(30)

            except Exception as e:
                print(f"Error fetching games: {e}")
                await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("\nShutting down pipeline...")
    finally:
        if "query" in locals() and query and query.isActive:
            query.stop()
        espn_connector.close()
        spark.stop()


if __name__ == "__main__":
    asyncio.run(run_game_pipeline())
