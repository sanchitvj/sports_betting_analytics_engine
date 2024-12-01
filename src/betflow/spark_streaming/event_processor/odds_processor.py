import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    count,
    from_json,
    current_timestamp,
    first,
    avg,
    last,
    stddev,
    lit,
    max as spark_max,
    min as spark_min,
    expr,
    to_timestamp,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType,
    ArrayType,
    IntegerType,
)
from betflow.spark_streaming.event_transformer import OddsTransformer


class OddsProcessor:
    """Process real-time odds data streams."""

    def __init__(
        self,
        spark: SparkSession,
        input_topic: str,
        output_topic: str,
        checkpoint_location: str,
        window_duration: int,
    ):
        self.spark = spark
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.checkpoint_location = checkpoint_location
        self.window_duration = window_duration
        self.logger = logging.getLogger(self.__class__.__name__)

        self.transformer = OddsTransformer()

    @staticmethod
    def _get_schema() -> StructType:
        """Define schema for transformed odds data from Kafka."""
        return StructType(
            [
                StructField("game_id", StringType(), True),
                StructField("sport_key", StringType(), True),
                StructField("sport_title", StringType(), True),
                StructField("commence_time", StringType(), True),
                StructField("home_team", StringType(), True),
                StructField("away_team", StringType(), True),
                # Odds by Bookmaker
                StructField(
                    "home_odds_by_bookie",
                    ArrayType(
                        StructType(
                            [
                                StructField("bookie_key", StringType(), True),
                                StructField("price", DoubleType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField(
                    "away_odds_by_bookie",
                    ArrayType(
                        StructType(
                            [
                                StructField("bookie_key", StringType(), True),
                                StructField("price", DoubleType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                # Pre-calculated Analytics
                StructField("best_home_odds", DoubleType(), True),
                StructField("best_away_odds", DoubleType(), True),
                # StructField("min_home_odds", DoubleType(), True),
                # StructField("min_away_odds", DoubleType(), True),
                StructField("avg_home_odds", DoubleType(), True),
                StructField("avg_away_odds", DoubleType(), True),
                StructField("bookmakers_count", IntegerType(), True),
                StructField("last_update", StringType(), True),
                StructField("timestamp", DoubleType(), True),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply transformations."""
        try:
            # First parse the JSON
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self._get_schema()).alias("data")
            ).select("data.*")

            # transformed_df = self.transformer.transform_odds_data(parsed_df)

            return parsed_df.withColumn("processing_time", current_timestamp())

        except Exception as e:
            self.logger.error(f"Error in parse and transform: {e}")
            raise

    def process(self):
        """Process odds stream with comprehensive analytics."""
        try:
            # Read from Kafka
            stream_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", self.input_topic)
                .option("startingOffsets", "latest")
                # .option("maxOffsetsPerTrigger", 10000)
                .load()
            )

            parsed_df = (
                self._parse_and_transform(stream_df)
                .withColumn("game_time", to_timestamp(col("commence_time")))
                .filter(
                    col("game_time").between(
                        current_timestamp(),
                        current_timestamp() + expr("INTERVAL 5 HOURS"),
                    )
                )
            )

            analytics_df = (
                parsed_df.withWatermark("processing_time", "1 minute")
                .groupBy(
                    window(
                        col("processing_time"), f"{self.window_duration} minutes"
                    ),  # "1 minute"),
                    "game_id",
                    "sport_key",
                )
                .agg(
                    # Game Info
                    col("window.start").alias("timestamp"),
                    first(col("sport_title")).alias("sport_title"),
                    first(col("commence_time")).alias("commence_time"),
                    first(col("home_team")).alias("home_team"),
                    first(col("away_team")).alias("away_team"),
                    # Basic Odds Analysis
                    first(col("best_home_odds")).alias("current_home_odds"),
                    first(col("best_away_odds")).alias("current_away_odds"),
                    avg(col("best_home_odds")).alias("avg_home_odds"),
                    avg(col("best_away_odds")).alias("avg_away_odds"),
                    spark_max(col("best_home_odds")).alias("max_home_odds"),
                    spark_max(col("best_away_odds")).alias("max_away_odds"),
                    spark_min(col("best_home_odds")).alias("min_home_odds"),
                    spark_min(col("best_away_odds")).alias("min_away_odds"),
                    # Market Efficiency
                    (
                        spark_max(col("best_home_odds"))
                        - spark_min(col("best_home_odds"))
                    ).alias("home_odds_spread"),
                    (
                        spark_max(col("best_away_odds"))
                        - spark_min(col("best_away_odds"))
                    ).alias("away_odds_spread"),
                    # Volatility Metrics
                    stddev(col("best_home_odds")).alias("home_odds_volatility"),
                    stddev(col("best_away_odds")).alias("away_odds_volatility"),
                    # Bookmaker Analysis
                    first(col("home_odds_by_bookie")).alias("home_odds_by_bookie"),
                    first(col("away_odds_by_bookie")).alias("away_odds_by_bookie"),
                    avg(col("bookmakers_count")).alias("avg_bookmakers"),
                    spark_max(col("bookmakers_count")).alias("max_bookmakers"),
                    # Time-based Metrics
                    first(col("last_update")).alias("first_update"),
                    last(col("last_update")).alias("latest_update"),
                    count("*").alias("update_count"),
                    # Implied Probability
                    (lit(1) / avg(col("best_home_odds"))).alias("implied_home_prob"),
                    (lit(1) / avg(col("best_away_odds"))).alias("implied_away_prob"),
                    # Market Movement
                    (
                        (
                            spark_max(col("best_home_odds"))
                            - spark_min(col("best_home_odds"))
                        )
                        / spark_min(col("best_home_odds"))
                    ).alias("home_odds_movement_pct"),
                    (
                        (
                            spark_max(col("best_away_odds"))
                            - spark_min(col("best_away_odds"))
                        )
                        / spark_min(col("best_away_odds"))
                    ).alias("away_odds_movement_pct"),
                )
            )

            kafka_query = (
                analytics_df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.output_topic)
                .option("checkpointLocation", f"{self.checkpoint_location}")
                # .option("cleanSource", "delete-on-success")
                .outputMode("update")
                # .trigger(processingTime="1 minute")
                .start()
            )

            return kafka_query

        except Exception as e:
            self.logger.error(f"Error processing odds stream: {e}")
            raise
