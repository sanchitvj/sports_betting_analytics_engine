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
    FloatType,
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
                StructField("home_team_record", StringType(), True),
                StructField("home_passing_display_value", StringType(), True),
                StructField("home_passing_value", FloatType(), True),
                StructField("home_passing_athlete_name", StringType(), True),
                StructField("home_passing_athlete_position", StringType(), True),
                StructField("home_rushing_display_value", StringType(), True),
                StructField("home_rushing_value", FloatType(), True),
                StructField("home_rushing_athlete_name", StringType(), True),
                StructField("home_rushing_athlete_position", StringType(), True),
                StructField("home_receive_display_value", StringType(), True),
                StructField("home_receive_value", FloatType(), True),
                StructField("home_receive_athlete_name", StringType(), True),
                StructField("home_receive_athlete_position", StringType(), True),
                StructField(
                    "home_team_linescores", ArrayType(DoubleType(), True), True
                ),
                # Away team
                StructField("away_team_name", StringType(), True),
                StructField("away_team_abbrev", StringType(), True),
                StructField("away_team_score", StringType(), True),
                # Away team statistics
                StructField("away_team_record", StringType(), True),
                StructField("away_passing_display_value", StringType(), True),
                StructField("away_passing_value", FloatType(), True),
                StructField("away_passing_athlete_name", StringType(), True),
                StructField("away_passing_athlete_position", StringType(), True),
                StructField("away_rushing_display_value", StringType(), True),
                StructField("away_rushing_value", FloatType(), True),
                StructField("away_rushing_athlete_name", StringType(), True),
                StructField("away_rushing_athlete_position", StringType(), True),
                StructField("away_receive_display_value", StringType(), True),
                StructField("away_receive_value", FloatType(), True),
                StructField("away_receive_athlete_name", StringType(), True),
                StructField("away_receive_athlete_position", StringType(), True),
                StructField(
                    "away_team_linescores", ArrayType(DoubleType(), True), True
                ),
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

            transformed_df = self.transformers.transform_espn_nba(parsed_df)

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
                    first("status_state").alias("status_state"),
                    first("status_detail").alias("status_detail"),
                    first("status_description").alias("status_description"),
                    first("period").alias("current_period"),
                    first("clock").alias("time_remaining"),
                    # Team Scores
                    first(col("home_team_name")).alias("home_team_name"),
                    first(col("away_team_name")).alias("away_team_name"),
                    last(col("home_team_score")).alias("home_team_score"),
                    last(col("away_team_score")).alias("away_team_score"),
                    # home Team Statistics
                    first(col("home_fg_pct")).alias("home_fg_pct"),
                    first(col("home_three_pt_pct")).alias("home_three_pt_pct"),
                    first(col("home_team_free_throws")).alias("home_team_free_throws"),
                    first(col("home_team_rebounds")).alias("home_team_rebounds"),
                    first(col("home_team_assists")).alias("home_team_assists"),
                    first(col("home_team_steals")).alias("home_team_steals"),
                    first(col("home_team_blocks")).alias("home_team_blocks"),
                    first(col("home_team_turnovers")).alias("home_team_turnovers"),
                    # away
                    first(col("away_fg_pct")).alias("away_fg_pct"),
                    first(col("away_three_pt_pct")).alias("away_three_pt_pct"),
                    first(col("away_team_free_throws")).alias("away_team_free_throws"),
                    first(col("away_team_rebounds")).alias("away_team_rebounds"),
                    first(col("away_team_assists")).alias("away_team_assists"),
                    first(col("away_team_steals")).alias("away_team_steals"),
                    first(col("away_team_blocks")).alias("away_team_blocks"),
                    first(col("away_team_turnovers")).alias("away_team_turnovers"),
                    # Game Analytics
                    (
                        last(col("home_team_score")) - first(col("home_team_score"))
                    ).alias("home_team_scoring_run"),
                    (
                        last(col("away_team_score")) - first(col("away_team_score"))
                    ).alias("away_team_scoring_run"),
                    # Additional Stats
                    first(col("home_rebounds")).alias("home_team_rebounds"),
                    first(col("away_rebounds")).alias("away_team_rebounds"),
                    first(col("home_assists")).alias("home_team_assists"),
                    first(col("away_assists")).alias("away_team_assists"),
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
