import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    when,
    avg,
    max,
    first,
    lit,
    coalesce,
    current_timestamp,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    FloatType,
    TimestampType,
)
from betflow.spark_streaming.event_transformer import WeatherTransformer


class WeatherProcessor:
    """Process real-time weather data streams with analytics."""

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
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

        self.transformer = WeatherTransformer()

    def _get_schema(self) -> StructType:
        """Get schema for weather data."""
        return StructType(
            [
                StructField("weather_id", StringType(), False),
                StructField("venue_id", StringType(), False),
                StructField("game_id", StringType(), False),
                StructField("game_name", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("temperature", FloatType(), False),
                StructField("feels_like", FloatType(), True),
                StructField("humidity", FloatType(), True),
                StructField("pressure", FloatType(), True),
                StructField("wind_speed", FloatType(), True),
                StructField("wind_direction", FloatType(), True),
                StructField("weather_condition", StringType(), True),
                StructField("weather_description", StringType(), True),
                StructField("visibility", FloatType(), True),
                StructField("clouds", FloatType(), True),
                StructField("state_code", StringType(), True),
                StructField("location", StringType(), True),
            ]
        )

    def _parse_and_transform(self, stream_df: DataFrame) -> DataFrame:
        """Parse JSON data and apply transformations."""

        def debug_batch(batch_df, batch_id):
            print(f"\nBatch ID: {batch_id}")
            print("\nSchema:")
            batch_df.printSchema()
            print("\nData:")
            batch_df.show(truncate=False)

        try:
            parsed_df = stream_df.select(
                from_json(col("value").cast("string"), self._get_schema()).alias("data")
            ).select("data.*")

            transformed_df = self.transformer.transform_openweather(parsed_df)

            return transformed_df.withColumn("processing_time", current_timestamp())

        except Exception as e:
            self.logger.error(f"Error in parse and transform: {e}")
            raise

    def process(self):
        """Start processing weather stream."""
        try:
            # Read from Kafka
            stream_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", self.input_topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                # .option("maxOffsetsPerTrigger", 100)
                .load()
            )

            parsed_df = self._parse_and_transform(stream_df)

            # Combine all analytics in a single aggregation. Calculate severity score first as a numeric value
            severity_expr = (
                when(col("visibility") < lit(5000), 2).otherwise(0)
                + when(col("wind_speed") > lit(20), 2).otherwise(0)
                + when(col("clouds") > lit(80), 1).otherwise(0)
            )

            analytics_df = (
                parsed_df.withWatermark("processing_time", "1 minute")
                .groupBy(
                    window(col("processing_time"), "20 minutes"),
                    "venue_id",
                    "game_id",
                    "game_name",
                    "location",
                    "state_code",
                )
                .agg(
                    # Temperature Analytics
                    col("window.start").alias("timestamp"),
                    avg("temperature").alias("avg_temperature"),
                    avg("feels_like").alias("avg_feels_like"),
                    ((avg("temperature") + avg("feels_like")) / 2).alias(
                        "comfort_index"
                    ),
                    avg("humidity").alias("avg_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    # Wind Analytics
                    avg("wind_speed").alias("avg_wind_speed"),
                    first("wind_direction").alias("last_wind_direction"),
                    (avg("wind_speed") * 0.7 + max("wind_speed") * 0.3).alias(
                        "wind_impact_score"
                    ),
                    # Condition Analytics
                    first("weather_condition").alias("current_condition"),
                    first("weather_description").alias("current_description"),
                    avg("visibility").alias("avg_visibility"),
                    coalesce(avg("clouds"), lit(0.0)).alias("avg_cloud_cover"),
                    # Weather Severity - now using pre-calculated score with col()
                    avg(severity_expr).cast("integer").alias("weather_severity"),
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

            return kafka_query  # , console_query

        except Exception as e:
            self.logger.error(f"Error processing weather stream: {e}")
            raise
