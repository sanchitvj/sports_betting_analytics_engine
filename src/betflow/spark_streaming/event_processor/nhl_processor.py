import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    current_timestamp,
    first,
    last,
    avg,
    sum as sum_,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    StructField,
    ArrayType,
    StructType,
    LongType,
    BooleanType,
)

from betflow.spark_streaming.event_transformer import GameTransformer


class NHLProcessor:
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
                StructField("home_team_abbrev", StringType(), True),
                StructField("home_team_score", StringType(), True),
                # Home team statistics
                StructField("home_team_saves", StringType(), True),
                StructField("home_team_save_pct", StringType(), True),
                StructField("home_team_goals", StringType(), True),
                StructField("home_team_assists", StringType(), True),
                StructField("home_team_points", StringType(), True),
                # StructField("home_team_penalties", StringType(), True),
                # StructField("home_team_penalty_minutes", StringType(), True),
                # StructField("home_team_power_plays", StringType(), True),
                # StructField("home_team_power_play_goals", StringType(), True),
                # StructField("home_team_power_play_pct", StringType(), True),
                StructField("home_team_record", StringType(), True),
                # Away team
                StructField("away_team_name", StringType(), True),
                StructField("away_team_abbrev", StringType(), True),
                StructField("away_team_score", StringType(), True),
                # Away team statistics
                StructField("away_team_saves", StringType(), True),
                StructField("away_team_save_pct", StringType(), True),
                StructField("away_team_goals", StringType(), True),
                StructField("away_team_assists", StringType(), True),
                StructField("away_team_points", StringType(), True),
                # StructField("away_team_penalties", StringType(), True),
                # StructField("away_team_penalty_minutes", StringType(), True),
                # StructField("away_team_power_plays", StringType(), True),
                # StructField("away_team_power_play_goals", StringType(), True),
                # StructField("away_team_power_play_pct", StringType(), True),
                StructField("away_team_record", StringType(), True),
                # Venue information
                StructField("venue_name", StringType(), True),
                StructField("venue_city", StringType(), True),
                StructField("venue_state", StringType(), True),
                StructField("venue_indoor", BooleanType(), True),
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

            transformed_df = self.transformers.transform_espn_nhl(parsed_df)

            return transformed_df.withColumn("processing_time", current_timestamp())
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
                    first("status_state").alias("game_state"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Names and Scores
                    first("home_team_name").alias("home_team_name"),
                    first("home_team_abbrev").alias("home_team_abbrev"),
                    first("away_team_name").alias("away_team_name"),
                    first("away_team_abbrev").alias("away_team_abbrev"),
                    last("home_team_score").alias("home_team_score"),
                    last("away_team_score").alias("away_team_score"),
                    # Home Team Statistics
                    avg(col("home_saves").cast("double")).alias("home_saves"),
                    avg(col("home_save_pct").cast("double")).alias("home_save_pct"),
                    sum_(col("home_goals").cast("double")).alias("home_goals"),
                    sum_(col("home_assists").cast("double")).alias("home_assists"),
                    sum_(col("home_points").cast("double")).alias("home_points"),
                    first("home_team_record").alias("home_team_record"),
                    # Away Team Statistics
                    avg(col("away_saves").cast("double")).alias("away_saves"),
                    avg(col("away_save_pct").cast("double")).alias("away_save_pct"),
                    sum_(col("away_goals").cast("double")).alias("away_goals"),
                    sum_(col("away_assists").cast("double")).alias("away_assists"),
                    sum_(col("away_points").cast("double")).alias("away_points"),
                    first("away_team_record").alias("away_team_record"),
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
                .trigger(processingTime="30 seconds")
                .start()
            )

            return query

        except Exception as e:
            self.logger.error(f"Error processing games stream: {e}")
            raise
