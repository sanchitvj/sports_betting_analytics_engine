import asyncio
from pyspark.sql import SparkSession
from betflow.api_connectors import OddsAPIConnector
import logging
import shutil
from betflow.spark_streaming.event_processor import OddsProcessor
from betflow.pipeline_utils import get_live_odds


async def run_odds_pipeline(base_ckpt: str, sport: str):
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

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Initialize connector
        odds_connector = OddsAPIConnector(
            api_key="a9fb527ba489184e92779cdf4a572631",
            kafka_bootstrap_servers="localhost:9092",
        )

        # Get initial odds data
        odds_data = get_live_odds(odds_connector, sport)
        if not odds_data:
            logger.info("No odds data found")
            return

        logger.info(f"Found odds for {len(odds_data)} games")

        # Start processor
        sport_code = sport.split("_")[1] if "_" in sport else sport
        sport_code = "cfb" if sport_code == "ncaaf" else sport_code
        odds_processor = OddsProcessor(
            spark=spark,
            input_topic=f"{sport_code}.odds.live",
            output_topic=f"{sport_code}_odds_analytics",
            checkpoint_location=f"{base_ckpt}/{sport_code}",
        )

        kafka_query = odds_processor.process()
        logger.info("Started odds streaming query")

        # Continuous fetch loop
        while kafka_query.isActive:
            try:
                if odds_data:
                    result = await odds_connector.fetch_and_publish_odds(
                        sport=sport, topic_name=f"{sport_code}.odds.live"
                    )
                    # logger.info(f"Published {sport} odds data.")

                kafka_query.processAllAvailable()
                # logger.info("Processed available odds data")
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Error in odds fetch loop: {e}")
                await asyncio.sleep(180)

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise
    finally:
        if "kafka_query" in locals() and kafka_query.isActive:
            kafka_query.stop()
        odds_connector.close()
        spark.stop()


async def main(base_ckpt):
    """Run odds pipelines for multiple sports concurrently."""
    sports_config = [
        "basketball_nba",
        "icehockey_nhl",
        "americanfootball_nfl",
        "americanfootball_ncaaf",
    ]

    tasks = [run_odds_pipeline(base_ckpt, sport) for sport in sports_config]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    base_ckpt = "/tmp/checkpoint/odds"
    try:
        shutil.rmtree(base_ckpt)
    except FileNotFoundError:
        print("Checkpoint directory doesn't exist")

    asyncio.run(main(base_ckpt))
