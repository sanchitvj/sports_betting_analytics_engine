import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    current_timestamp,
    first,
    last,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    ArrayType,
    StructType,
    LongType,
    DoubleType,
    BooleanType,
)

from betflow.spark_streaming.event_transformer import GameTransformer


class NFLProcessor:
    """Process real-time game data streams with analytics."""

    def __init__(
        self,
        spark: SparkSession,
        input_topic: str,
        output_topic: str,
        checkpoint_location: str,
    ):
        self.spark = spark
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.checkpoint_location = checkpoint_location
        self.logger = logging.getLogger(self.__class__.__name__)

        self.transformers = GameTransformer()

    @staticmethod
    def _get_schema() -> StructType:
        """Define schema for NFL game data."""
        return StructType(
            [
                StructField("game_id", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("status_state", StringType(), True),
                StructField("status_detail", StringType(), True),
                StructField("status_description", StringType(), True),
                StructField("period", IntegerType(), True),
                StructField("clock", StringType(), True),
                # Team Info
                StructField("home_team_name", StringType(), True),
                StructField("home_team_abbreviation", StringType(), True),
                StructField("home_team_score", IntegerType(), True),
                StructField("home_team_record", StringType(), True),
                StructField("home_team_linescores", ArrayType(DoubleType()), True),
                StructField("away_team_name", StringType(), True),
                StructField("away_team_abbreviation", StringType(), True),
                StructField("away_team_score", IntegerType(), True),
                StructField("away_team_record", StringType(), True),
                StructField("away_team_linescores", ArrayType(DoubleType()), True),
                # Leaders
                StructField("passing_leader_name", StringType(), True),
                StructField("passing_leader_value", StringType(), True),
                StructField("passing_leader_team", StringType(), True),
                StructField("rushing_leader_name", StringType(), True),
                StructField("rushing_leader_value", StringType(), True),
                StructField("rushing_leader_team", StringType(), True),
                StructField("receiving_leader_name", StringType(), True),
                StructField("receiving_leader_value", StringType(), True),
                StructField("receiving_leader_team", StringType(), True),
                # Venue
                StructField("venue_name", StringType(), True),
                StructField("venue_city", StringType(), True),
                StructField("venue_state", StringType(), True),
                StructField("venue_indoor", BooleanType(), True),
                StructField("broadcasts", ArrayType(StringType()), True),
                StructField("timestamp", LongType(), True),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply sport-specific transformations."""
        try:
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self._get_schema()).alias("data")
            ).select("data.*")

            # transformed_df = self.transformers.transform_espn_nba(parsed_df)

            return parsed_df.withColumn("processing_time", current_timestamp())

        except Exception as e:
            self.logger.error(f"Error in parse and transform: {e}")
            raise

    def process(self):
        """Start processing game data stream with analytics."""
        try:
            stream_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", self.input_topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
            )

            parsed_df = self._parse_and_transform(stream_df)
            analytics_df = (
                parsed_df.withWatermark("processing_time", "1 minute")
                .groupBy(
                    window(col("processing_time"), "3 minutes"),
                    "game_id",
                    "venue_name",
                    "venue_city",
                    "venue_state",
                    "broadcasts",
                )
                .agg(
                    # Game Status
                    col("window.start").alias("timestamp"),
                    first("status_state").alias("status_state"),
                    first("status_detail").alias("status_detail"),
                    first("status_description").alias("status_description"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Info
                    first("home_team_name").alias("home_team_name"),
                    first("home_team_abbreviation").alias("home_team_abbreviation"),
                    first("away_team_name").alias("away_team_name"),
                    first("away_team_abbreviation").alias("away_team_abbreviation"),
                    # Scoring
                    last("home_team_score").alias("home_team_score"),
                    last("away_team_score").alias("away_team_score"),
                    first("home_team_record").alias("home_team_record"),
                    first("away_team_record").alias("away_team_record"),
                    first("home_team_linescores").alias("home_team_quarters"),
                    first("away_team_linescores").alias("away_team_quarters"),
                    # Game Leaders
                    first("passing_leader_name").alias("passing_leader"),
                    first("passing_leader_value").alias("passing_stats"),
                    first("passing_leader_team").alias("passing_team"),
                    first("rushing_leader_name").alias("rushing_leader"),
                    first("rushing_leader_value").alias("rushing_stats"),
                    first("rushing_leader_team").alias("rushing_team"),
                    first("receiving_leader_name").alias("receiving_leader"),
                    first("receiving_leader_value").alias("receiving_stats"),
                    first("receiving_leader_team").alias("receiving_team"),
                    # Scoring Analysis
                    (last("home_team_score") - first("home_team_score")).alias(
                        "home_scoring_run"
                    ),
                    (last("away_team_score") - first("away_team_score")).alias(
                        "away_scoring_run"
                    ),
                )
            )

            query = (
                analytics_df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.output_topic)
                .option("checkpointLocation", self.checkpoint_location)
                .outputMode("update")
                .start()
            )

            return query

        except Exception as e:
            self.logger.error(f"Error processing games stream: {e}")
            raise
