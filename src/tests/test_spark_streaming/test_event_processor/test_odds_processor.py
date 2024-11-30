import pytest
from datetime import datetime
import json
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, lit

from betflow.spark_streaming.event_processor import OddsProcessor


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class TestOddsProcessor:
    @pytest.fixture(scope="session")
    def spark(self):
        """Create SparkSession for testing."""
        return (
            SparkSession.builder.appName("test_odds_processor")
            .master("local[2]")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .getOrCreate()
        )

    @pytest.fixture
    def odds_processor(self, spark):
        """Create OddsProcessor instance."""
        return OddsProcessor(
            spark=spark,
            input_topic="test_odds_input",
            output_topic="test_odds_output",
            checkpoint_location="/tmp/test_checkpoint",
        )

    @pytest.fixture
    def sample_odds_data(self):
        """Create sample odds data."""
        current_time = datetime.now()
        return {
            "odds_id": "odds123",
            "game_id": "game123",
            "sport_type": "NBA",
            "bookmaker_id": "odds_api",
            "processing_time": current_time.isoformat(),
            "timestamp": datetime.now(),
            "market_type": "h2h",
            "odds_value": 1.95,
            "spread_value": -5.5,
            "total_value": 220.5,
            "probability": 0.513,
            "volume": {"total": 10000.0, "matched": 8000.0},
            "movement": {"opening": 1.90, "current": 1.95},
            "status": "active",
            "metadata": {
                "bookmaker_name": "Test Bookmaker",
                "market_name": "Money Line",
            },
        }

    def test_schema_creation(self, odds_processor):
        """Test schema creation."""
        schema = odds_processor._get_schema()

        # Verify schema fields
        field_names = [f.name for f in schema.fields]
        assert "odds_id" in field_names
        assert "game_id" in field_names
        assert "bookmaker_id" in field_names
        assert "odds_value" in field_names

    def test_odds_analytics(self, odds_processor, spark, sample_odds_data):
        """Test odds analytics calculations."""
        # Create DataFrame with proper schema
        df = spark.createDataFrame(
            [
                {
                    "game_id": "game123",
                    "bookmaker_id": "odds_api",
                    "market_type": "h2h",
                    "odds_value": 1.95,
                    "timestamp": datetime.now(),
                    "processing_time": datetime.now(),
                    "volume": {"total": 10000.0, "matched": 8000.0},
                    "movement": {"opening": 1.90, "current": 1.95},
                }
            ]
        )

        # Add window column
        df = df.withColumn(
            "processing_time", col("processing_time").cast("timestamp")
        ).withColumn("window", window(col("processing_time"), "5 minutes"))

        # Apply analytics
        result_df = odds_processor._apply_odds_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert row.avg_odds == pytest.approx(1.95, rel=1e-2)
        assert row.max_odds == pytest.approx(1.95, rel=1e-2)
        assert row.min_odds == pytest.approx(1.95, rel=1e-2)
        assert row.total_volume == 10000.0
        assert row.avg_matched_volume == 8000.0

    def test_probability_analytics(self, odds_processor, spark, sample_odds_data):
        """Test probability analytics calculations."""
        # Create DataFrame with proper schema
        df = spark.createDataFrame(
            [
                {
                    "game_id": "game123",
                    "bookmaker_id": "odds_api",
                    "probability": 0.513,
                    "processing_time": datetime.now(),
                }
            ]
        )

        # Add window column
        df = df.withColumn(
            "processing_time", col("processing_time").cast("timestamp")
        ).withColumn("window", window(col("processing_time"), "5 minutes"))

        # Apply analytics
        result_df = odds_processor._apply_probability_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert row.avg_probability == pytest.approx(0.513, rel=1e-2)
        assert row.max_probability == pytest.approx(0.513, rel=1e-2)
        assert row.min_probability == pytest.approx(0.513, rel=1e-2)
        assert row.probability_spread == pytest.approx(0.0, rel=1e-2)

    def test_market_analytics(self, odds_processor, spark, sample_odds_data):
        """Test market analytics calculations."""
        # Create test DataFrame
        df = spark.createDataFrame([sample_odds_data])
        df = df.withColumn("processing_time", col("processing_time").cast("timestamp"))

        # Apply analytics
        result_df = odds_processor._apply_market_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert row.bookmaker_count == 1
        assert len(row.market_types) == 1
        assert row.total_market_volume == 10000.0

    def test_parse_and_transform(self, odds_processor, spark, sample_odds_data):
        """Test data parsing and transformation."""
        # Create input DataFrame with proper JSON encoding
        input_df = spark.createDataFrame(
            [{"key": 1, "value": json.dumps(sample_odds_data, cls=DateTimeEncoder)}]
        )

        # Mock transformer methods
        with patch.object(
            odds_processor.transformer, "transform_odds_api"
        ) as mock_odds_api, patch.object(
            odds_processor.transformer, "transform_pinnacle"
        ) as mock_pinnacle:
            mock_odds_api.return_value = sample_odds_data
            mock_pinnacle.return_value = sample_odds_data

            # Apply transformation
            result_df = odds_processor._parse_and_transform(input_df)

            # Verify schema and data
            assert "odds_id" in result_df.columns
            assert "bookmaker_id" in result_df.columns
            assert "processing_time" in result_df.columns

            # Verify transformer calls
            if sample_odds_data["bookmaker_id"] == "odds_api":
                mock_odds_api.assert_called_once()
            elif sample_odds_data["bookmaker_id"] == "pinnacle":
                mock_pinnacle.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_stream(self, odds_processor, spark, sample_odds_data):
        """Test end-to-end stream processing."""
        # Constants
        TABLE_NAME = "test_odds_table"
        query = None

        try:
            # Create test data DataFrame
            test_data = spark.createDataFrame([sample_odds_data])

            # Create streaming input DataFrame using memory format
            input_df = (
                spark.readStream.format("rate")  # Use rate source for testing
                .option("rowsPerSecond", 1)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .withColumn(
                    "value", lit(json.dumps(sample_odds_data, cls=DateTimeEncoder))
                )
            )

            # Mock the analytics methods
            with patch.object(
                odds_processor, "_parse_and_transform"
            ) as mock_parse, patch.object(
                odds_processor, "_apply_odds_analytics"
            ) as mock_odds, patch.object(
                odds_processor, "_apply_probability_analytics"
            ) as mock_prob, patch.object(
                odds_processor, "_apply_market_analytics"
            ) as mock_market:
                # Set return values
                mock_parse.return_value = test_data
                mock_odds.return_value = test_data
                mock_prob.return_value = test_data
                mock_market.return_value = test_data

                # Start the streaming query
                query = (
                    input_df.writeStream.format("memory")  # Use memory sink for testing
                    .queryName(TABLE_NAME)
                    .outputMode("append")
                    .start()
                )

                # Wait for some data to be processed
                query.processAllAvailable()

                # Verify results
                results = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()

                # Verify method calls
                mock_parse.assert_called()
                mock_odds.assert_called()
                mock_prob.assert_called()
                mock_market.assert_called()

                # Verify data presence
                assert len(results) > 0, "No results found in output table"

                # Verify data content if needed
                if len(results) > 0:
                    row = results[0]
                    assert "value" in row.__fields__, "Value column missing"

        except Exception as e:
            pytest.fail(f"Stream processing test failed: {str(e)}")

        finally:
            # Clean up resources
            if query and query.isActive:
                query.stop()

            # Drop temporary table
            try:
                spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            except Exception as e:
                print(f"Cleanup failed: {str(e)}")
