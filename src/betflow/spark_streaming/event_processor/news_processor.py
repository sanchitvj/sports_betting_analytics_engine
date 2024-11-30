import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    window,
    avg,
    sum,
    count,
    collect_list,
    size,
    from_json,
    current_timestamp,
)
from pyspark.sql.types import (
    StringType,
    TimestampType,
    StructField,
    MapType,
    ArrayType,
    StructType,
    FloatType,
)
from betflow.spark_streaming.event_transformer import NewsTransformer


class NewsProcessor:
    """Process real-time news data streams."""

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

        self.transformer = NewsTransformer()

    def _get_schema(self) -> StructType:
        """Get schema for news data."""
        return StructType(
            [
                StructField("news_id", StringType(), False),
                StructField("source", StringType(), False),
                StructField("published_date", TimestampType(), False),
                StructField("title", StringType(), False),
                StructField("content", StringType(), False),
                StructField("url", StringType(), False),
                StructField("author", StringType(), True),
                StructField("categories", ArrayType(StringType()), False),
                StructField(
                    "entities",
                    MapType(StringType(), ArrayType(StringType()), True),
                    False,
                ),
                StructField("sentiment_score", FloatType(), True),
                StructField("relevance_score", FloatType(), True),
                StructField("related_games", ArrayType(StringType()), True),
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
            if row_dict.get("source") == "newsapi":
                transformed_data = self.transformer.transform_newsapi(row_dict)
            elif row_dict.get("source") == "gnews":
                transformed_data = self.transformer.transform_gnews(row_dict)
            elif row_dict.get("source") == "rss":
                transformed_data = self.transformer.transform_rss(row_dict)
            else:
                transformed_data = row_dict
            transformed_rows.append(transformed_data)

        # Create DataFrame from transformed data
        transformed_df = self.spark.createDataFrame(transformed_rows)

        # Add processing timestamp
        return transformed_df.withColumn("processing_time", current_timestamp())

    def _apply_sentiment_analytics(self, df: DataFrame) -> DataFrame:
        """Calculate sentiment analytics."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(window(col("processing_time"), "5 minutes"), "source")
            .agg(
                # Average sentiment by source
                avg("sentiment_score").alias("avg_sentiment"),
                # Count of articles
                count("news_id").alias("article_count"),
                # Collect titles
                collect_list("title").alias("article_titles"),
                # Entity mentions
                sum(size(col("entities.teams"))).alias("team_mentions"),
                sum(size(col("entities.players"))).alias("player_mentions"),
            )
        )

    def _apply_relevance_analytics(self, df: DataFrame) -> DataFrame:
        """Calculate relevance metrics."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(window(col("processing_time"), "5 minutes"))
            .agg(
                # Average relevance score
                avg("relevance_score").alias("avg_relevance"),
                # High relevance articles
                count(when(col("relevance_score") > 0.8, True)).alias(
                    "high_relevance_count"
                ),
                # Categories distribution
                collect_list("categories").alias("category_distribution"),
            )
        )

    def _apply_entity_analytics(self, df: DataFrame) -> DataFrame:
        """Analyze entity mentions."""
        return (
            df.withWatermark("processing_time", "1 minute")
            .groupBy(window(col("processing_time"), "5 minutes"))
            .agg(
                # Entity counts
                collect_list("entities").alias("entity_mentions"),
                # Related games
                collect_list("related_games").alias("game_connections"),
                # Entity type counts
                sum(size(col("entities.teams"))).alias("total_team_mentions"),
                sum(size(col("entities.players"))).alias("total_player_mentions"),
                sum(size(col("entities.locations"))).alias("total_location_mentions"),
            )
        )

    def process(self):
        """Start processing news stream."""
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

            # Apply analytics
            sentiment_analytics = self._apply_sentiment_analytics(windowed_df)
            relevance_analytics = self._apply_relevance_analytics(windowed_df)
            entity_analytics = self._apply_entity_analytics(windowed_df)

            # Combine analytics
            analytics_df = sentiment_analytics.join(
                relevance_analytics, ["window"]
            ).join(entity_analytics, ["window"])

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

            return query

        except Exception as e:
            self.logger.error(f"Error processing news stream: {e}")
            raise
