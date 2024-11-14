import pytest
from datetime import datetime
from unittest.mock import patch, Mock, PropertyMock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, struct, lit, expr
from pyspark.sql.types import StructType
from betflow.spark_streaming.event_processor import GamesProcessor

current_time = datetime.now()


class TestGamesProcessor:
    @pytest.fixture(scope="session")
    def spark(self):
        """Create SparkSession for testing."""
        return (
            SparkSession.builder.appName("test_games_processor")
            .master("local[2]")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            )
            .getOrCreate()
        )

    @pytest.fixture
    def games_processor(self, spark):
        """Create GamesProcessor instance."""
        return GamesProcessor(
            spark=spark,
            input_topic="test_games_input",
            output_topic="test_games_output",
            checkpoint_location="/tmp/test_checkpoint",
        )

    @pytest.fixture
    def sample_game_data(self):
        """Create sample game data."""
        return {
            "game_id": "game123",
            "sport_type": "NBA",
            "start_time": datetime.now().isoformat(),
            "venue_id": "venue123",
            "status": "active",
            "home_team_id": "home123",
            "away_team_id": "away123",
            "season": 2024,
            "season_type": "regular",
            "current_period": 2,
            "time_remaining": "5:30",
            "score": {"home_team_id": 105, "away_team_id": 98},
            "stats": {
                "shooting": {
                    "field_goals": 45.5,
                    "attempts": 85.0,
                    "three_pointers": 12.0,
                },
                "rebounds": {
                    "offensive": 10.0,
                    "defensive": 30.0,
                    "total": 40.0,
                },  # passed test_team_analytics by modifying 10 -> 10.0
                "defense": {
                    "sacks": 0.0  # For NFL
                },
                "passing": {"yards": 0.0},
                "rushing": {"yards": 0.0},
                "batting": {"hits": 0.0},
                "pitching": {"strikeouts": 0.0},
                "player": {  # Add player stats
                    "points": 25.0,
                    "assists": 8.0,
                    "rebounds": 5.0,
                    "field_goals_made": 10.0,
                    "field_goals_attempted": 20.0,
                },
            },
            "processing_time": datetime.now().isoformat(),
        }

    def test_schema_creation(self, games_processor):
        """Test schema creation."""
        schema = games_processor._get_schema()

        assert isinstance(schema, StructType)
        assert "game_id" in [f.name for f in schema.fields]
        assert "sport_type" in [f.name for f in schema.fields]
        assert "score" in [f.name for f in schema.fields]
        assert "stats" in [f.name for f in schema.fields]

    def test_parse_and_transform(self, games_processor, spark, sample_game_data):
        """Test data parsing and transformation."""
        # Create input DataFrame
        input_df = spark.createDataFrame([(1, str(sample_game_data))], ["key", "value"])

        # Create expected transformed data
        transformed_data = {
            "game_id": sample_game_data["game_id"],
            "sport_type": "NBA",  # Assuming NBA for this test
            "home_team_id": sample_game_data["home_team_id"],
            "away_team_id": sample_game_data["away_team_id"],
            "score": sample_game_data["score"],
            "stats": sample_game_data["stats"],
        }

        # Mock transformer methods
        with patch.object(
            games_processor.transformers["NBA"],
            "transform",
            return_value=transformed_data,
        ) as mock_nba_transform, patch.object(
            games_processor.transformers["NFL"],
            "transform",
            return_value=transformed_data,
        ) as mock_nfl_transform, patch.object(
            games_processor.transformers["MLB"],
            "transform",
            return_value=transformed_data,
        ) as mock_mlb_transform:
            # Apply transformation
            result_df = games_processor._parse_and_transform(input_df)

            # Verify schema and data
            assert "game_id" in result_df.columns
            assert "sport_type" in result_df.columns
            assert "processing_time" in result_df.columns

            # Check data
            row = result_df.collect()[0]
            assert row.game_id == sample_game_data["game_id"]
            assert row.sport_type == sample_game_data["sport_type"]

            # Verify appropriate transformer was called
            if sample_game_data["sport_type"] == "NBA":
                mock_nba_transform.assert_called_once()
            elif sample_game_data["sport_type"] == "NFL":
                mock_nfl_transform.assert_called_once()
            elif sample_game_data["sport_type"] == "MLB":
                mock_mlb_transform.assert_called_once()

    def test_score_analytics(self, games_processor, spark, sample_game_data):
        """Test score analytics calculations."""
        # Create input DataFrame
        df = spark.createDataFrame([sample_game_data])

        # Apply analytics
        result_df = games_processor._apply_score_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check calculations
        assert "score_progression" in row.__fields__
        assert "max_lead" in row.__fields__
        assert "home_scoring_rate" in row.__fields__
        assert row.max_lead >= 0

    def test_team_analytics(self, games_processor, spark, sample_game_data):
        """Test team performance analytics."""
        # Create DataFrame with proper schema
        df = spark.createDataFrame([sample_game_data])

        # Add processing_time column if not present
        if "processing_time" not in df.columns:
            df = df.withColumn("processing_time", current_timestamp())

        # Apply analytics
        result_df = games_processor._apply_team_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check team metrics
        assert row.offensive_efficiency is not None
        assert row.defensive_metric is not None  # Should be average defensive rebounds

        # Verify specific metrics for NBA
        if row.sport_type == "NBA":
            # Offensive efficiency should be (field_goals / attempts) * 100
            expected_efficiency = (45.5 / 85.0) * 100
            assert abs(row.offensive_efficiency - expected_efficiency) < 0.01

            # Defensive metric should be average defensive rebounds
            assert row.defensive_metric == 30.0

    def test_period_analytics(self, games_processor, spark, sample_game_data):
        """Test period/quarter analysis."""
        # Create DataFrame with all required fields
        df = spark.createDataFrame([sample_game_data])
        df = df.withColumn("processing_time", col("processing_time").cast("timestamp"))

        # Apply analytics
        result_df = games_processor._apply_period_analytics(df)

        # Verify results
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]

        # Check period scores
        assert row.home_period_score == 105
        assert row.away_period_score == 98

        # Check sport-specific stats
        if row.sport_type == "NBA":
            assert row.period_stats["fg_rate"] == 45.5
            assert row.period_stats["three_pt_rate"] == 12.0

    @pytest.mark.asyncio
    async def test_process_stream(self, games_processor, spark, sample_game_data):
        """Test end-to-end stream processing."""
        current_time = datetime.now()

        # Create DataFrame with window column
        base_df = spark.createDataFrame(
            [
                {
                    "game_id": sample_game_data["game_id"],
                    "sport_type": sample_game_data["sport_type"],
                    "processing_time": current_time,
                    "stats": sample_game_data["stats"],
                    "score": sample_game_data["score"],
                }
            ]
        )

        # Add window column
        windowed_df = base_df.withColumn(
            "window",
            struct(
                lit(current_time).alias("start"),
                (lit(current_time) + expr("INTERVAL 5 MINUTES")).alias("end"),
            ),
        )

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

        # Mock DataStreamReader
        mock_reader = Mock()
        mock_reader.format = Mock(return_value=mock_reader)
        mock_reader.option = Mock(return_value=mock_reader)
        mock_reader.load = Mock(return_value=mock_df)

        # Mock readStream property
        mock_read_stream = PropertyMock(return_value=mock_reader)
        type(spark).readStream = mock_read_stream

        try:
            # Mock analytics methods to return windowed DataFrame
            with patch.object(
                games_processor, "_parse_and_transform", return_value=windowed_df
            ) as mock_parse, patch.object(
                games_processor, "_apply_score_analytics", return_value=windowed_df
            ) as mock_score, patch.object(
                games_processor, "_apply_team_analytics", return_value=windowed_df
            ) as mock_team, patch.object(
                games_processor, "_apply_player_analytics", return_value=windowed_df
            ) as mock_player, patch.object(
                games_processor, "_apply_period_analytics", return_value=windowed_df
            ) as mock_period:
                # Start processing
                query = games_processor.process()

                # Verify streaming setup
                assert query.isActive

                # Verify method calls
                mock_parse.assert_called_once()
                mock_score.assert_called_once()
                mock_team.assert_called_once()
                mock_player.assert_called_once()
                mock_period.assert_called_once()

                # Verify Kafka configuration
                mock_df.writeStream.format.assert_called_with("kafka")
                mock_df.writeStream.option.assert_any_call(
                    "kafka.bootstrap.servers", "localhost:9092"
                )
                mock_df.writeStream.option.assert_any_call(
                    "topic", games_processor.output_topic
                )

        finally:
            if "query" in locals() and query and query.isActive:
                query.stop()

    def test_error_handling(self, games_processor, spark):
        """Test error handling in processing."""
        # Create invalid input data
        invalid_data = spark.createDataFrame([(1, "invalid_json")], ["key", "value"])

        # Verify error handling
        with pytest.raises(Exception):
            games_processor._parse_and_transform(invalid_data)

    def test_windowing(self, games_processor, spark, sample_game_data):
        """Test windowed aggregations."""
        # Create time-series data
        df = spark.createDataFrame([sample_game_data])

        # Apply windowed analytics
        result_df = games_processor._apply_score_analytics(df)

        # Verify window calculations
        result = result_df.collect()
        assert len(result) > 0
        row = result[0]
        assert "window" in row.__fields__
