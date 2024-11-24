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
        .master("local[2]")
        .config("spark.sql.streaming.schemaInference", "true")
        # State Store configs
        # .config(
        #     "spark.sql.streaming.stateStore.providerClass",
        #     "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        # )
        # .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10")
        # .config(
        #     "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"
        # )
        # .config("spark.sql.streaming.minBatchesToRetain", "10")
        # .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
        # # Memory Management
        # .config("spark.memory.offHeap.enabled", "true")
        # .config("spark.memory.offHeap.size", "1g")
        # .config("spark.driver.memory", "4g")
        # .config("spark.executor.memory", "4g")
        # .config("spark.rpc.message.maxSize", "1024")
        # .config("spark.driver.maxResultSize", "8g")
        # .config("spark.executor.heartbeatInterval", "20s")
        # .config("spark.scheduler.mode", "FAIR")
        # .config("spark.streaming.backpressure.enabled", "true")
        .config(
            "spark.jars.packages",
            (
                # Kafka integration
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.apache.kafka:kafka-clients:3.5.1,"
                # # Iceberg integration
                # "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,"
                # "org.apache.iceberg:iceberg-aws-bundle:1.7.0,"
                # "software.amazon.awssdk:bundle:2.26.14,"
                # "software.amazon.awssdk:url-connection-client:2.26.14,"
                # # AWS dependencies
                # "org.apache.hadoop:hadoop-aws:3.3.4,"
                # "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                # "org.apache.spark:spark-hadoop-cloud_2.13:3.5.3,"
                # # Scala dependencies
                # "org.scala-lang:scala-library:2.12.18,"
                # "org.scala-lang:scala-reflect:2.12.18,"
                # "org.scala-lang:scala-compiler:2.12.18"
            ),
        )
        # .config(
        #     "spark.sql.extensions",
        #     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        # )
        # # Add Hadoop AWS configurations
        # .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # # .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        # # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config(
        #     "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
        # )
        # .config(
        #     "spark.sql.catalog.glue_catalog.catalog-impl",
        #     "org.apache.iceberg.aws.glue.GlueCatalog",
        # )
        # .config(
        #     "spark.sql.catalog.glue_catalog.warehouse", "s3://cur-sp-data-aeb/iceberg"
        # )
        # .config(
        #     "spark.sql.catalog.glue_catalog.io-impl",
        #     "org.apache.iceberg.aws.s3.S3FileIO",
        # )
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
                    logger.info(f"Published {sport} odds data.")

                kafka_query.processAllAvailable()
                logger.info("Processed available odds data")
                await asyncio.sleep(1800)

            except Exception as e:
                logger.error(f"Error in odds fetch loop: {e}")
                await asyncio.sleep(180)

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
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
