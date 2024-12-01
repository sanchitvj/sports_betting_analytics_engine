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
)


class NBAProcessor:
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

    @staticmethod
    def _get_schema() -> StructType:
        """Get schema for NBA game data."""
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
                # Home team statistics
                StructField("home_team_field_goals", StringType(), True),
                StructField("home_team_three_pointers", StringType(), True),
                StructField("home_team_free_throws", StringType(), True),
                StructField("home_team_rebounds", StringType(), True),
                StructField("home_team_assists", StringType(), True),
                # Away team
                StructField("away_team_name", StringType(), True),
                StructField("away_team_id", StringType(), True),
                StructField("away_team_abbreviation", StringType(), True),
                StructField("away_team_score", StringType(), True),
                # Away team statistics
                StructField("away_team_field_goals", StringType(), True),
                StructField("away_team_three_pointers", StringType(), True),
                StructField("away_team_free_throws", StringType(), True),
                StructField("away_team_rebounds", StringType(), True),
                StructField("away_team_assists", StringType(), True),
                # Venue information
                StructField("venue_name", StringType(), True),
                StructField("venue_city", StringType(), True),
                StructField("venue_state", StringType(), True),
                # Broadcasts and timestamp
                StructField("broadcasts", ArrayType(StringType(), True), True),
                StructField("timestamp", LongType(), True),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply sport-specific transformations."""
        try:
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self._get_schema()).alias("data")
            ).select("data.*")

            # parsed_df = self.transformers.transform_espn_nba(parsed_df)

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

            # Single aggregation for all analytics
            analytics_df = (
                parsed_df.withWatermark("processing_time", "1 minute")
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
                    first("status_state").alias("status_state"),
                    first("status_detail").alias("status_detail"),
                    first("status_description").alias("status_description"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Scores
                    last(col("home_team_score")).alias("home_team_score"),
                    last(col("away_team_score")).alias("away_team_score"),
                    # home Team Statistics
                    first(col("home_team_field_goals")).alias("home_fg_pct"),
                    first(col("home_team_three_pointers")).alias("home_three_pt_pct"),
                    first(col("home_team_free_throws")).alias("home_ft_pct"),
                    first(col("home_team_rebounds")).alias("home_rebounds"),
                    first(col("home_team_assists")).alias("home_assists"),
                    # away
                    first(col("away_team_field_goals")).alias("away_fg_pct"),
                    first(col("away_team_three_pointers")).alias("away_three_pt_pct"),
                    first(col("away_team_free_throws")).alias("away_ft_pct"),
                    first(col("away_team_rebounds")).alias("away_rebounds"),
                    first(col("away_team_assists")).alias("away_assists"),
                    # Game Analytics
                    (
                        last(col("home_team_score")) - first(col("home_team_score"))
                    ).alias("home_team_scoring_run"),
                    (
                        last(col("away_team_score")) - first(col("away_team_score"))
                    ).alias("away_team_scoring_run"),
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
