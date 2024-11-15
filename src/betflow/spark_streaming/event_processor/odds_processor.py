import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    avg,
    sum,
    count,
    collect_list,
    from_json,
    current_timestamp,
    struct,
    expr,
    lit,
    max as max_,
    min as min_,
)
from pyspark.sql.types import (
    StringType,
    TimestampType,
    StructField,
    MapType,
    StructType,
    FloatType,
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
    ):
        self.spark = spark
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.checkpoint_location = checkpoint_location
        self.logger = logging.getLogger(self.__class__.__name__)

        self.transformer = OddsTransformer()

    @staticmethod
    def _get_schema() -> StructType:
        """Get schema for odds data."""
        return StructType(
            [
                StructField("odds_id", StringType(), False),
                StructField("game_id", StringType(), False),
                StructField("sport_type", StringType(), False),
                StructField("bookmaker_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("market_type", StringType(), False),
                StructField("odds_value", FloatType(), False),
                StructField("spread_value", FloatType(), True),
                StructField("total_value", FloatType(), True),
                StructField("probability", FloatType(), True),
                StructField("volume", MapType(StringType(), FloatType(), True), True),
                StructField("movement", MapType(StringType(), FloatType(), True), True),
                StructField("status", StringType(), False),
                StructField(
                    "metadata", MapType(StringType(), StringType(), True), True
                ),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply transformations."""
        # First parse the JSON
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), self._get_schema()).alias("data")
        ).select("data.*")

        # Create a list to store transformed rows
        transformed_rows = []

        # Process each row using the appropriate transformer
        for row in parsed_df.collect():
            row_dict = row.asDict()
            if row_dict.get("bookmaker_id") == "odds_api":
                transformed_data = self.transformer.transform_odds_api(
                    row_dict, row_dict["sport_type"]
                )
            elif row_dict.get("bookmaker_id") == "pinnacle":
                transformed_data = self.transformer.transform_pinnacle(
                    row_dict, row_dict["sport_type"]
                )
            else:
                transformed_data = row_dict
            transformed_rows.append(transformed_data)

        # Create DataFrame from transformed data
        transformed_df = self.spark.createDataFrame(transformed_rows)

        # Add processing timestamp
        return transformed_df.withColumn("processing_time", current_timestamp())

    @staticmethod
    def _apply_odds_analytics(df: DataFrame) -> DataFrame:
        """Calculate odds analytics."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"),
                "game_id",
                "bookmaker_id",
                "market_type",
            )
            .agg(
                # Average odds
                avg(col("odds_value")).alias("avg_odds"),
                # Odds movement
                max_(col("odds_value")).alias("max_odds"),
                min_(col("odds_value")).alias("min_odds"),
                # Volume metrics
                sum(expr("volume.total")).alias("total_volume"),
                avg(expr("volume.matched")).alias("avg_matched_volume"),
                # Market depth
                count(lit(1)).alias("market_depth"),
                # Collect movements
                collect_list(
                    struct(col("timestamp"), col("odds_value"), col("movement"))
                ).alias("odds_history"),
            )
        )

    @staticmethod
    def _apply_probability_analytics(df: DataFrame) -> DataFrame:
        """Analyze implied probabilities."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(
                window(col("processing_time"), "5 minutes"), "game_id", "bookmaker_id"
            )
            .agg(
                # Average probability
                avg(col("probability")).alias("avg_probability"),
                # Probability range
                max_(col("probability")).alias("max_probability"),
                min_(col("probability")).alias("min_probability"),
                # Market efficiency
                (max_(col("probability")) - min_(col("probability"))).alias(
                    "probability_spread"
                ),
            )
        )

    def _apply_market_analytics(self, df: DataFrame) -> DataFrame:
        """Analyze market behavior."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(window(col("processing_time"), "5 minutes"), "game_id")
            .agg(
                # Market statistics
                count("bookmaker_id").alias("bookmaker_count"),
                collect_list("market_type").alias("market_types"),
                # Volume analysis
                sum(expr("volume.total")).alias("total_market_volume"),
                # Movement analysis
                collect_list(
                    struct(col("bookmaker_id"), col("market_type"), col("movement"))
                ).alias("market_movements"),
            )
        )

    def process(self):
        """Start processing odds stream."""
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

            # Add window column for analytics
            windowed_df = parsed_df.withColumn(
                "window", window(col("processing_time"), "5 minutes")
            )

            # Apply analytics with windowed DataFrame
            odds_analytics = self._apply_odds_analytics(windowed_df)
            probability_analytics = self._apply_probability_analytics(windowed_df)
            market_analytics = self._apply_market_analytics(windowed_df)

            # Combine analytics using common columns
            analytics_df = odds_analytics.join(
                probability_analytics, ["window", "game_id", "bookmaker_id"]
            ).join(market_analytics, ["window", "game_id"])

            # Write to Kafka
            query = (
                analytics_df.selectExpr("to_json(struct(*)) AS value")
                .writeStream.format("iceberg")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", self.output_topic)
                .option("checkpointLocation", self.checkpoint_location)
                .outputMode("update")
                .start()
            )

            return query

        except Exception as e:
            self.logger.error(f"Error processing odds stream: {e}")
            raise
