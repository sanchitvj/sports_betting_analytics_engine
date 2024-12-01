import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, window, first, last, current_timestamp
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    ArrayType,
    StructType,
    LongType,
    DoubleType,
)


class CFBProcessor:
    """Process college football game data stream."""

    def __init__(
        self,
        spark: SparkSession,
        input_topic: str,
        output_topic: str,
        checkpoint_location: str,
    ):
        """Initialize processor."""
        self.spark = spark

        self.input_topic = input_topic
        self.output_topic = output_topic
        self.checkpoint_location = checkpoint_location
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _get_schema() -> StructType:
        """Get schema for CFB game data."""
        return StructType(
            [
                # Game identification
                StructField("game_id", StringType(), True),
                StructField("start_time", StringType(), True),
                # Game status
                StructField("status_state", StringType(), True),
                StructField("status_detail", StringType(), True),
                StructField("status_description", StringType(), True),
                StructField("period", IntegerType(), True),
                StructField("clock", StringType(), True),
                # Home team
                StructField("home_team_name", StringType(), True),
                StructField("home_team_id", StringType(), True),
                StructField("home_team_abbreviation", StringType(), True),
                StructField("home_team_score", StringType(), True),
                StructField("home_team_record", StringType(), True),
                StructField("home_team_linescores", ArrayType(DoubleType()), True),
                # Away team
                StructField("away_team_name", StringType(), True),
                StructField("away_team_id", StringType(), True),
                StructField("away_team_abbreviation", StringType(), True),
                StructField("away_team_score", StringType(), True),
                StructField("away_team_record", StringType(), True),
                StructField("away_team_linescores", ArrayType(DoubleType()), True),
                # leaders
                StructField("passing_leader_name", StringType(), True),
                StructField("passing_leader_display_value", StringType(), True),
                StructField("passing_leader_value", DoubleType(), True),
                StructField("passing_leader_team", StringType(), True),
                StructField("rushing_leader_name", StringType(), True),
                StructField("rushing_leader_display_value", StringType(), True),
                StructField("rushing_leader_value", DoubleType(), True),
                StructField("rushing_leader_team", StringType(), True),
                StructField("receiving_leader_name", StringType(), True),
                StructField("receiving_leader_display_value", StringType(), True),
                StructField("receiving_leader_value", DoubleType(), True),
                StructField("receiving_leader_team", StringType(), True),
                # Venue information
                StructField("venue_name", StringType(), True),
                StructField("venue_city", StringType(), True),
                StructField("venue_state", StringType(), True),
                # Additional information
                StructField("broadcasts", ArrayType(StringType(), True), True),
                StructField("timestamp", LongType(), True),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply transformations."""
        try:
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self._get_schema()).alias("data")
            ).select("data.*")

            return parsed_df.withColumn("processing_time", current_timestamp())

        except Exception as e:
            self.logger.error(f"Error in parse and transform: {e}")
            raise

    def process(self):
        """Process stream and write to both Kafka and Druid."""
        try:
            # Read from Kafka
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
                parsed_df.withWatermark("processing_time", "1 minutes")
                .groupBy(
                    window(col("processing_time"), "3 minutes"),
                    "game_id",
                    "venue_name",
                    "venue_city",
                    "venue_state",
                    "broadcasts",
                    "home_team_name",
                    "home_team_id",
                    "home_team_abbreviation",
                    "away_team_name",
                    "away_team_id",
                    "away_team_abbreviation",
                )
                .agg(
                    # Game Status
                    col("window.start").alias("timestamp"),
                    first("status_state").alias("game_state"),
                    first("status_detail").alias("status_detail"),
                    first("status_description").alias("status_description"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Scores
                    last("home_team_score").alias("home_team_score"),
                    last("away_team_score").alias("away_team_score"),
                    first("home_team_record").alias("home_team_record"),
                    first("away_team_record").alias("away_team_record"),
                    first("home_team_linescores").alias("home_team_linescores"),
                    first("away_team_linescores").alias("away_team_linescores"),
                    # Team Leaders
                    first("passing_leader_value").alias("passing_leader_value"),
                    first("rushing_leader_value").alias("rushing_leader_value"),
                    first("receiving_leader_value").alias("receiving_leader_value"),
                    first("passing_leader_name").alias("passing_leader_name"),
                    first("rushing_leader_name").alias("rushing_leader_name"),
                    first("receiving_leader_name").alias("receiving_leader_name"),
                    first("passing_leader_team").alias("passing_leader_team"),
                    first("rushing_leader_team").alias("rushing_leader_team"),
                    first("receiving_leader_team").alias("receiving_leader_team"),
                    first("passing_leader_display_value").alias(
                        "passing_leader_display_value"
                    ),
                    first("rushing_leader_display_value").alias(
                        "rushing_leader_display_value"
                    ),
                    first("receiving_leader_display_value").alias(
                        "receiving_leader_display_value"
                    ),
                    # Game Analytics
                    (last("home_team_score") - first("home_team_score")).alias(
                        "home_scoring_run"
                    ),
                    (last("away_team_score") - first("away_team_score")).alias(
                        "away_scoring_run"
                    ),
                )
            )

            kafka_query = (
                analytics_df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.output_topic)
                .option("checkpointLocation", self.checkpoint_location)
                .outputMode("update")
                # .trigger(processingTime="30 seconds")
                .start()
            )

            return kafka_query

        except Exception as e:
            self.logger.error(f"Error processing games stream: {e}")
            raise
