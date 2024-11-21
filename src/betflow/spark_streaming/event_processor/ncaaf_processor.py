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
)
from betflow.spark_streaming.event_transformer import GameTransformer


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
        self.logger = logging.getLogger(self.__class__.__name__)

        self.transformers = GameTransformer()

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
                StructField("home_team_abbrev", StringType(), True),
                StructField("home_team_score", StringType(), True),
                StructField("home_team_record", StringType(), True),
                # Home team leaders
                StructField("home_passing_leader", StringType(), True),
                StructField("home_rushing_leader", StringType(), True),
                StructField("home_receiving_leader", StringType(), True),
                # Away team
                StructField("away_team_name", StringType(), True),
                StructField("away_team_abbrev", StringType(), True),
                StructField("away_team_score", StringType(), True),
                StructField("away_team_record", StringType(), True),
                # Away team leaders
                StructField("away_passing_leader", StringType(), True),
                StructField("away_rushing_leader", StringType(), True),
                StructField("away_receiving_leader", StringType(), True),
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

            transformed_df = self.transformers.transform_espn_cfb(parsed_df)

            return transformed_df.withColumn("processing_time", current_timestamp())

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
                    first("status_state").alias("game_state"),
                    first("status_detail").alias("status_detail"),
                    first("status_description").alias("status_description"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Names and Scores
                    first("home_team_name").alias("home_team_name"),
                    # first("home_team_abbrev").alias("home_team_abbrev"),
                    first("away_team_name").alias("away_team_name"),
                    # first("away_team_abbrev").alias("away_team_abbrev"),
                    last("home_team_score").alias("home_team_score"),
                    last("away_team_score").alias("away_team_score"),
                    # Team Records
                    first("home_team_record").alias("home_team_record"),
                    first("away_team_record").alias("away_team_record"),
                    # Team Leaders
                    first("home_passing_leader").alias("home_passing_leader"),
                    first("home_rushing_leader").alias("home_rushing_leader"),
                    first("home_receiving_leader").alias("home_receiving_leader"),
                    first("away_passing_leader").alias("away_passing_leader"),
                    first("away_rushing_leader").alias("away_rushing_leader"),
                    first("away_receiving_leader").alias("away_receiving_leader"),
                    # Game Analytics
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
