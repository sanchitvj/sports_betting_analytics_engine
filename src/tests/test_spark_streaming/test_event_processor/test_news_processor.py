import pytest
from datetime import datetime
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from betflow.spark_streaming.event_processor import NewsProcessor
import json


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class TestNewsProcessor:
    @pytest.fixture(scope="session")
    def spark(self):
        """Create SparkSession for testing."""
        return (
            SparkSession.builder.appName("test_news_processor")
            .master("local[2]")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @pytest.fixture
    def news_processor(self, spark):
        """Create NewsProcessor instance."""
        return NewsProcessor(
            spark=spark,
            input_topic="test_news_input",
            output_topic="test_news_output",
            checkpoint_location="/tmp/test_checkpoint",
        )

    @pytest.fixture
    def sample_news_data(self):
        """Create sample news data."""
        current_time = datetime.now()
        return {
            "news_id": "news123",
            "source": "newsapi",
            "published_date": current_time,
            "title": "Test News Article",
            "content": "This is a test article content",
            "url": "https://test.com/article",
            "author": "Test Author",
            "categories": ["sports", "basketball"],
            "entities": {
                "teams": ["Lakers", "Celtics"],
                "players": ["LeBron James"],
                "locations": ["Los Angeles"],
            },
            "sentiment_score": 0.8,
            "relevance_score": 0.9,
            "related_games": ["game123"],
            "metadata": {"source_id": "src123"},
            "processing_time": current_time,
        }

    def test_schema_creation(self, news_processor):
        """Test schema creation."""
        schema = news_processor._get_schema()

        # Verify schema fields
        field_names = [f.name for f in schema.fields]
        assert "news_id" in field_names
        assert "source" in field_names
        assert "sentiment_score" in field_names
        assert "entities" in field_names

    def test_sentiment_analytics(self, news_processor, spark, sample_news_data):
        """Test sentiment analytics calculations."""
        # Create test DataFrame
        df = spark.createDataFrame([sample_news_data])
        df = df.withColumn("processing_time", col("processing_time").cast("timestamp"))

        # Apply analytics
        result_df = news_processor._apply_sentiment_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert row.avg_sentiment == 0.8
        assert row.article_count == 1
        assert len(row.article_titles) == 1
        assert row.team_mentions == 2
        assert row.player_mentions == 1

    def test_relevance_analytics(self, news_processor, spark, sample_news_data):
        """Test relevance analytics calculations."""
        # Create test DataFrame
        df = spark.createDataFrame([sample_news_data])
        df = df.withColumn("processing_time", col("processing_time").cast("timestamp"))

        # Apply analytics
        result_df = news_processor._apply_relevance_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert row.avg_relevance == 0.9
        assert row.high_relevance_count == 1
        assert len(row.category_distribution) == 1

    def test_entity_analytics(self, news_processor, spark, sample_news_data):
        """Test entity analytics calculations."""
        # Create test DataFrame
        df = spark.createDataFrame([sample_news_data])
        df = df.withColumn("processing_time", col("processing_time").cast("timestamp"))

        # Apply analytics
        result_df = news_processor._apply_entity_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert len(row.entity_mentions) == 1
        assert len(row.game_connections) == 1
        assert row.total_team_mentions == 2
        assert row.total_player_mentions == 1
        assert row.total_location_mentions == 1

    def test_parse_and_transform(self, news_processor, spark, sample_news_data):
        """Test data parsing and transformation."""
        # Ensure source is set in sample data
        sample_news_data["source"] = "newsapi"

        # Create input DataFrame with proper JSON encoding
        input_df = spark.createDataFrame(
            [{"key": 1, "value": json.dumps(sample_news_data, cls=DateTimeEncoder)}]
        )

        # Create expected transformed data
        transformed_data = {
            "news_id": "test_id",
            "source": "newsapi",
            "title": sample_news_data["title"],
            "content": sample_news_data["content"],
            "published_date": sample_news_data["published_date"],
            "url": sample_news_data.get("url", ""),
            "author": sample_news_data.get("author"),
            "categories": sample_news_data.get("categories", []),
            "entities": sample_news_data.get("entities", {}),
            "sentiment_score": sample_news_data.get("sentiment_score"),
            "relevance_score": sample_news_data.get("relevance_score"),
            "related_games": sample_news_data.get("related_games", []),
            "metadata": sample_news_data.get("metadata", {}),
        }

        # Mock transformer method
        with patch.object(
            news_processor.transformer,
            "transform_newsapi",
            return_value=transformed_data,
        ) as mock_transform:
            # Apply transformation
            result_df = news_processor._parse_and_transform(input_df)

            # Verify schema and data
            assert "news_id" in result_df.columns
            assert "source" in result_df.columns
            assert "processing_time" in result_df.columns

            # Verify transformer was called
            mock_transform.assert_called_once()

            # Verify transformed data
            result = result_df.collect()
            assert len(result) > 0
            row = result[0]
            assert row.news_id == "test_id"
            assert row.source == "newsapi"
            assert row.title == sample_news_data["title"]

    @pytest.mark.asyncio
    async def test_process_stream(self, news_processor, spark, sample_news_data):
        """Test end-to-end stream processing."""
        # Create mock streaming DataFrame
        mock_df = Mock()
        mock_df.isStreaming = True
        mock_df.writeStream = Mock()
        mock_df.writeStream.format = Mock(return_value=mock_df.writeStream)
        mock_df.writeStream.option = Mock(return_value=mock_df.writeStream)
        mock_df.writeStream.outputMode = Mock(return_value=mock_df.writeStream)
        mock_df.writeStream.start = Mock(
            return_value=Mock(
                isActive=True, awaitTermination=Mock(return_value=None), stop=Mock()
            )
        )

        # Create parsed DataFrame
        parsed_df = spark.createDataFrame([sample_news_data])
        parsed_df_mock = Mock(wraps=parsed_df)
        parsed_df_mock.isStreaming = True

        # Mock the stream processing
        with patch("pyspark.sql.streaming.DataStreamReader") as mock_reader, patch(
            "pyspark.sql.DataFrame.writeStream"
        ) as mock_write_stream:
            # Configure mock reader
            mock_reader.return_value = Mock()
            mock_reader.return_value.format = Mock(
                return_value=mock_reader.return_value
            )
            mock_reader.return_value.option = Mock(
                return_value=mock_reader.return_value
            )
            mock_reader.return_value.load = Mock(return_value=mock_df)

            # Configure mock writer
            mock_write_stream.return_value = mock_df.writeStream

            # Mock analytics methods
            with patch.object(
                news_processor, "_parse_and_transform", return_value=parsed_df_mock
            ) as mock_parse, patch.object(
                news_processor,
                "_apply_sentiment_analytics",
                return_value=parsed_df_mock,
            ) as mock_sentiment, patch.object(
                news_processor,
                "_apply_relevance_analytics",
                return_value=parsed_df_mock,
            ) as mock_relevance, patch.object(
                news_processor, "_apply_entity_analytics", return_value=parsed_df_mock
            ) as mock_entity:
                try:
                    # Start processing
                    query = news_processor.process()

                    # Verify streaming setup
                    assert query.isActive

                    # Verify method calls
                    mock_parse.assert_called_once()
                    mock_sentiment.assert_called_once()
                    mock_relevance.assert_called_once()
                    mock_entity.assert_called_once()

                    # Verify Kafka configuration
                    mock_write_stream.return_value.format.assert_called_with("kafka")
                    mock_write_stream.return_value.option.assert_any_call(
                        "kafka.bootstrap.servers", "localhost:9092"
                    )
                    mock_write_stream.return_value.option.assert_any_call(
                        "topic", news_processor.output_topic
                    )

                finally:
                    if "query" in locals() and query and query.isActive:
                        query.stop()
