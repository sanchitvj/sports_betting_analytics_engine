import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    struct,
    window,
    expr,
    when,
    current_timestamp,
    avg,
    max,
    min,
    sum,
    count,
    collect_list,
    lit,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    TimestampType,
    StructField,
    MapType,
    ArrayType,
    StructType,
    FloatType,
)

from betflow.spark_streaming.event_transformer import (
    NFLGameTransformer,
    NBAGameTransformer,
    MLBGameTransformer,
)


class GamesProcessor:
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

        # Initialize transformers
        self.transformers = {
            "NFL": NFLGameTransformer(),
            "NBA": NBAGameTransformer(),
            "MLB": MLBGameTransformer(),
        }

    @staticmethod
    def _get_schema() -> StructType:
        """Get schema for game data."""
        return StructType(
            [
                # Base game fields
                StructField("game_id", StringType(), False),
                StructField("sport_type", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("venue_id", StringType(), False),
                StructField("status", StringType(), False),
                StructField("home_team_id", StringType(), False),
                StructField("away_team_id", StringType(), False),
                StructField("season", IntegerType(), False),
                StructField("season_type", StringType(), False),
                StructField("broadcast", ArrayType(StringType()), True),
                # Sport-specific fields
                StructField("current_quarter", IntegerType(), True),  # NFL
                StructField("current_period", IntegerType(), True),  # NBA
                StructField("current_inning", IntegerType(), True),  # MLB
                StructField("time_remaining", StringType(), True),
                StructField("down", IntegerType(), True),  # NFL
                StructField("yards_to_go", IntegerType(), True),  # NFL
                StructField("possession", StringType(), True),  # NFL
                StructField("inning_half", StringType(), True),  # MLB
                StructField("outs", IntegerType(), True),  # MLB
                StructField("bases_occupied", ArrayType(IntegerType()), True),  # MLB
                # Score and stats
                StructField("score", MapType(StringType(), IntegerType(), True), False),
                StructField(
                    "stats",
                    MapType(
                        StringType(),  # Category (passing, rushing, shooting, etc.)
                        MapType(
                            StringType(),  # Stat name (yards, touchdowns, etc.)
                            FloatType(),  # Stat value
                            True,
                        ),
                        True,
                    ),
                    False,
                ),
                # Processing metadata
                StructField("processing_time", TimestampType(), False),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply sport-specific transformations."""
        # Parse the JSON
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), self._get_schema()).alias("data")
        ).select("data.*")

        # Apply transformations using when clauses
        transformed_df = parsed_df.select(
            "*",
            when(
                col("sport_type") == "NBA",
                struct(self.transformers["NBA"].transform(parsed_df)),
            )
            .when(
                col("sport_type") == "NFL",
                struct(self.transformers["NFL"].transform(parsed_df)),
            )
            .when(
                col("sport_type") == "MLB",
                struct(self.transformers["MLB"].transform(parsed_df)),
            )
            .alias("transformed_data"),
        )

        # Add processing timestamp
        return transformed_df.withColumn("processing_time", current_timestamp())

    @staticmethod
    def _apply_score_analytics(df: DataFrame) -> DataFrame:
        """Track score progression and game momentum."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"), "game_id", "sport_type"
            )
            .agg(
                # Score progression
                collect_list(struct("processing_time", "score")).alias(
                    "score_progression"
                ),
                # Score differentials
                max(expr("score['home_team_id'] - score['away_team_id']")).alias(
                    "max_lead"
                ),
                min(expr("score['home_team_id'] - score['away_team_id']")).alias(
                    "min_lead"
                ),
                # Scoring rates
                (sum(expr("score['home_team_id']")) / count("*")).alias(
                    "home_scoring_rate"
                ),
                (sum(expr("score['away_team_id']")) / count("*")).alias(
                    "away_scoring_rate"
                ),
            )
        )

    @staticmethod
    def _apply_team_analytics(df: DataFrame) -> DataFrame:
        """Calculate team performance metrics with type casting."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"),
                "game_id",
                "sport_type",
                "home_team_id",
                "away_team_id",
            )
            .agg(
                # Offensive efficiency with type casting
                when(
                    col("sport_type") == "NBA",
                    (
                        expr("cast(sum(stats.shooting.field_goals) as double)")
                        / expr("cast(sum(stats.shooting.attempts) as double)")
                        * 100
                    ),
                )
                .when(
                    col("sport_type") == "NFL",
                    expr(
                        "cast(sum(stats.passing.yards) + sum(stats.rushing.yards) as double)"
                    ),
                )
                .alias("offensive_efficiency"),
                # Defensive metrics with type casting
                when(
                    col("sport_type") == "NBA",
                    expr("cast(avg(stats.rebounds.defensive) as double)"),
                )
                .when(
                    col("sport_type") == "NFL",
                    expr("cast(sum(stats.defense.sacks) as double)"),
                )
                .alias("defensive_metric"),
            )
        )

    @staticmethod
    def _apply_player_analytics(df: DataFrame) -> DataFrame:
        """Aggregate player statistics."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"),
                "game_id",
                "sport_type",
                "player_id",
            )
            .agg(
                # NBA player stats
                when(
                    col("sport_type") == "NBA",
                    struct(
                        sum(expr("stats.player.points")).alias("points"),
                        sum(expr("stats.player.assists")).alias("assists"),
                        sum(expr("stats.player.rebounds")).alias("rebounds"),
                        (
                            sum(expr("stats.player.field_goals_made"))
                            / sum(expr("stats.player.field_goals_attempted"))
                            * 100
                        ).alias("fg_percentage"),
                    ),
                ),
                # NFL player stats
                when(
                    col("sport_type") == "NFL",
                    struct(
                        sum(expr("stats.player.passing_yards")).alias("passing_yards"),
                        sum(expr("stats.player.rushing_yards")).alias("rushing_yards"),
                        sum(expr("stats.player.touchdowns")).alias("touchdowns"),
                        sum(expr("stats.player.completions")).alias("completions"),
                    ),
                ),
                # MLB player stats
                when(
                    col("sport_type") == "MLB",
                    struct(
                        avg(expr("stats.player.batting_average")).alias("batting_avg"),
                        sum(expr("stats.player.hits")).alias("hits"),
                        sum(expr("stats.player.runs")).alias("runs"),
                        sum(expr("stats.player.rbis")).alias("rbis"),
                    ),
                ),
            )
            .alias("player_stats")
        )

    @staticmethod
    def _apply_period_analytics(df: DataFrame) -> DataFrame:
        """Analyze quarter/period/inning performance."""
        # First, check which period columns exist
        available_columns = df.columns

        # Create game period based on available columns and sport type
        if "current_period" in available_columns:
            df = df.withColumn(
                "game_period",
                when(col("sport_type") == lit("NBA"), col("current_period")).otherwise(
                    lit(None)
                ),
            )
        elif "current_quarter" in available_columns:
            df = df.withColumn(
                "game_period",
                when(col("sport_type") == lit("NFL"), col("current_quarter")).otherwise(
                    lit(None)
                ),
            )
        elif "current_inning" in available_columns:
            df = df.withColumn(
                "game_period",
                when(col("sport_type") == lit("MLB"), col("current_inning")).otherwise(
                    lit(None)
                ),
            )
        else:
            # If no period column exists, create a default
            df = df.withColumn("game_period", lit(1))

        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"),
                "game_id",
                "sport_type",
                "game_period",
            )
            .agg(
                # Scoring by period
                sum(expr("score['home_team_id']")).alias("home_period_score"),
                sum(expr("score['away_team_id']")).alias("away_period_score"),
                # Create unified struct with all possible metrics
                struct(
                    # NBA metrics (will be null for other sports)
                    when(
                        col("sport_type") == lit("NBA"),
                        avg(expr("stats.shooting.field_goals")),
                    )
                    .otherwise(lit(None))
                    .alias("fg_rate"),
                    when(
                        col("sport_type") == lit("NBA"),
                        avg(expr("stats.shooting.three_pointers")),
                    )
                    .otherwise(lit(None))
                    .alias("three_pt_rate"),
                    # NFL metrics (will be null for other sports)
                    when(
                        col("sport_type") == lit("NFL"),
                        sum(expr("stats.passing.yards")),
                    )
                    .otherwise(lit(None))
                    .alias("passing_yards"),
                    when(
                        col("sport_type") == lit("NFL"),
                        sum(expr("stats.rushing.yards")),
                    )
                    .otherwise(lit(None))
                    .alias("rushing_yards"),
                    # MLB metrics (will be null for other sports)
                    when(
                        col("sport_type") == lit("MLB"), sum(expr("stats.batting.hits"))
                    )
                    .otherwise(lit(None))
                    .alias("hits"),
                    when(
                        col("sport_type") == lit("MLB"),
                        avg(expr("stats.pitching.strikeouts")),
                    )
                    .otherwise(lit(None))
                    .alias("strikeouts"),
                ).alias("period_stats"),
            )
        )

    @staticmethod
    def _preprocess_data(df: DataFrame) -> DataFrame:
        """Preprocess data before analytics."""
        # Cast numeric fields to proper types
        numeric_fields = [
            "stats.shooting.field_goals",
            "stats.shooting.attempts",
            "stats.rebounds.defensive",
            "stats.rebounds.offensive",
        ]

        for field in numeric_fields:
            df = df.withColumn(field, col(field).cast("double"))

        return df

    def process(self):
        """Start processing game data stream with analytics."""
        try:
            # Read from Kafka
            stream_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", self.input_topic)
                .option("startingOffsets", "latest")
                .load()
            )

            # Parse and transform data
            parsed_df = self._parse_and_transform(stream_df)
            # Preprocess data
            processed_df = self._preprocess_data(parsed_df)
            windowed_df = processed_df.withColumn(
                "window", window(col("processing_time"), "5 minutes")
            )
            # Apply analytics
            score_analytics = self._apply_score_analytics(windowed_df)
            team_analytics = self._apply_team_analytics(windowed_df)
            player_analytics = self._apply_player_analytics(windowed_df)
            period_analytics = self._apply_period_analytics(windowed_df)

            # Combine analytics
            analytics_df = (
                score_analytics.join(team_analytics, ["window", "game_id"])
                .join(player_analytics, ["window", "game_id"])
                .join(period_analytics, ["window", "game_id"])
            )

            # Write to Kafka
            query = (
                analytics_df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.output_topic)
                .option("checkpointLocation", self.checkpoint_location)
                .outputMode("update")
                .start()
            )
            if not hasattr(self, "output_format") or self.output_format == "kafka":
                query = (
                    query.option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", self.output_topic)
                    .option("checkpointLocation", self.checkpoint_location)
                )
            else:
                query = query.queryName("test_output")

            return query.start()

        except Exception as e:
            self.logger.error(f"Error processing games stream: {e}")
            raise
