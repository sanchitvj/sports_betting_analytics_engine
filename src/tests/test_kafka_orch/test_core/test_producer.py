import pytest
from unittest.mock import Mock, AsyncMock, patch, ANY
import asyncio
from betflow.kafka_orch.core.producer import SportsBettingProducer
from datetime import datetime


class TestSportsBettingProducer:
    @pytest.fixture
    def mock_config(self):
        config = Mock()
        config.bootstrap_servers = "localhost:9092"
        config.client_id = "test_client"
        config.odds_api_key = "test_odds_key"
        config.openweather_api_key = "test_weather_key"
        config.newsapi_key = "test_news_key"
        config.gnews_api_key = "test_gnews_key"
        config.rss_feeds = ["test_feed"]
        config.enable_metrics = True
        config.refresh_interval = 0.1
        config.topics = []
        return config

    @pytest.fixture
    def producer(self, mock_config):
        """Create producer with mocked dependencies."""
        # Mock KafkaAdminClient at the lowest level
        with patch("kafka.admin.KafkaAdminClient") as mock_kafka:
            # Mock KafkaAdminManager
            with patch(
                "betflow.kafka_orch.core.producer.KafkaAdminManager"
            ) as mock_admin:
                mock_admin_instance = mock_admin.return_value
                mock_admin_instance.ensure_topics_exist = Mock()
                mock_admin_instance.close = Mock()

                # Mock other dependencies
                with patch(
                    "betflow.kafka_orch.core.producer.KafkaErrorHandler"
                ) as mock_error, patch(
                    "betflow.kafka_orch.core.producer.HealthHandler"
                ) as mock_health, patch(
                    "betflow.kafka_orch.core.producer.MetricsCollector"
                ) as mock_metrics:
                    # Create producer
                    producer = SportsBettingProducer(mock_config)

                    # Replace _initialize_connectors with mock to prevent actual initialization
                    producer._initialize_connectors = Mock()

                    # Set up mock connectors directly
                    producer.espn_connector = AsyncMock()
                    producer.odds_api_connector = AsyncMock()
                    producer.weather_connectors = {
                        "openweather": AsyncMock(),
                        "openmeteo": AsyncMock(),
                    }
                    producer.news_connectors = {
                        "newsapi": AsyncMock(),
                        "gnews": AsyncMock(),
                        "rss": AsyncMock(),
                    }
                    producer._get_venues_for_sports = AsyncMock(
                        return_value=[{"city": "New York", "lat": 40.7, "lon": -74.0}]
                    )

                    return producer

    def test_initialization(self, producer, mock_config):
        """Test producer initialization."""
        assert producer.config == mock_config
        assert producer.admin_manager is not None
        assert producer.health_handler is not None
        assert producer.error_handler is not None

    def test_init_connector(self, producer):
        """Test connector initialization in isolation."""
        # Reset the mock to test just this call
        producer.health_handler.initialize_source.reset_mock()

        # Create test connector
        mock_connector = Mock()
        mock_connector.return_value = "test_connector"

        # Test initialization
        result = producer._init_connector("test_source", mock_connector)

        # Verify results
        assert result == "test_connector"
        producer.health_handler.initialize_source.assert_called_once_with("test_source")

    @pytest.fixture
    def valid_game_data(self):
        """Valid game data fixture."""
        return {
            "game_id": "123",
            "sport_type": "NFL",
            "start_time": datetime.now(),
            "venue_id": "venue_123",
            "status": "active",
            "home_team_id": "team1",
            "away_team_id": "team2",
            "season": 2024,
            "season_type": "regular",
            "broadcast": ["ESPN"],  # Required field
            "current_quarter": 2,
            "time_remaining": "10:30",
            "down": 2,
            "yards_to_go": 8,
            "possession": "team1",
            "score": {"team1": 14, "team2": 7},
            "stats": {"passing": {"yards": 200.0}, "rushing": {"yards": 100.0}},
        }

    @pytest.fixture
    def valid_odds_data(self):
        """Valid odds data fixture."""
        return {
            "odds_id": "odds_123",
            "game_id": "game_123",
            "sport_type": "NFL",
            "bookmaker_id": "book_123",
            "timestamp": datetime.now(),
            "market_type": "moneyline",
            "odds_value": 1.95,
            "spread_value": None,
            "total_value": None,
            "probability": 0.5,
            "volume": {"total": 1000.0},
            "movement": {"opening": 2.0, "current": 1.95},
            "status": "active",
            "metadata": {},
        }

    @pytest.fixture
    def valid_weather_data(self):
        """Valid weather data fixture."""
        return {
            "weather_id": "weather_123",
            "venue_id": "venue_123",
            "game_id": "game_123",
            "timestamp": datetime.now(),
            "temperature": 72.5,
            "feels_like": 74.0,
            "humidity": 65.0,
            "wind_speed": 5.0,
            "wind_direction": "NE",
            "precipitation_probability": 20.0,
            "weather_condition": "clear",
            "visibility": 10.0,
            "pressure": 1015.0,
            "uv_index": 5.0,
            "details": {},
        }

    @pytest.fixture
    def valid_news_data(self):
        """Valid news data fixture."""
        return {
            "news_id": "news_123",
            "source": "ESPN",
            "published_date": datetime.now(),
            "title": "Test News",
            "content": "Test content",
            "url": "http://example.com",
            "author": "John Doe",
            "categories": ["sports", "betting"],
            "entities": {"teams": ["team1", "team2"]},
            "sentiment_score": 0.8,
            "relevance_score": 0.9,
            "related_games": ["game_123"],
            "metadata": {},
        }

    @pytest.mark.asyncio
    async def test_fetch_weather_data(self, producer, valid_weather_data):
        """Test weather data fetching."""
        venue = {"city": "New York", "lat": 40.7, "lon": -74.0}

        # Mock the weather connector to return valid data
        producer.weather_connectors[
            "openweather"
        ].fetch_and_publish_weather.return_value = valid_weather_data

        # Patch the validation method to return the same data
        with patch.object(
            producer, "_validate_weather_data", return_value=valid_weather_data
        ):
            result = await producer.fetch_weather_data(venue)

            assert result == valid_weather_data
            producer.weather_connectors[
                "openweather"
            ].fetch_and_publish_weather.assert_called_once_with(venue["city"])

    def test_validate_game_data(self, producer, valid_game_data):
        """Test game data validation."""
        # Test NFL game data
        validated_data = producer._validate_game_data("football", valid_game_data)
        assert validated_data["game_id"] == valid_game_data["game_id"]
        assert validated_data["sport_type"] == valid_game_data["sport_type"]

        # Test invalid sport
        with pytest.raises(ValueError, match="Unsupported sport"):
            producer._validate_game_data("invalid_sport", valid_game_data)

        # Test invalid data
        invalid_data = {"game_id": "123"}  # Missing required fields
        with pytest.raises(Exception):
            producer._validate_game_data("football", invalid_data)

    def test_validate_odds_data(self, producer, valid_odds_data):
        """Test odds data validation."""
        validated_data = producer._validate_odds_data(valid_odds_data)
        assert validated_data["odds_id"] == valid_odds_data["odds_id"]
        assert validated_data["market_type"] == valid_odds_data["market_type"]

        # Test invalid data
        invalid_data = {"odds_id": "123"}  # Missing required fields
        with pytest.raises(Exception):
            producer._validate_odds_data(invalid_data)

    def test_validate_weather_data(self, producer, valid_weather_data):
        """Test weather data validation."""
        validated_data = producer._validate_weather_data(valid_weather_data)
        assert validated_data["weather_id"] == valid_weather_data["weather_id"]
        assert validated_data["temperature"] == valid_weather_data["temperature"]

        # Test invalid data
        invalid_data = {"weather_id": "123"}  # Missing required fields
        with pytest.raises(Exception):
            producer._validate_weather_data(invalid_data)

    def test_validate_news_data(self, producer, valid_news_data):
        """Test news data validation."""
        validated_data = producer._validate_news_data(valid_news_data)
        assert validated_data["news_id"] == valid_news_data["news_id"]
        assert validated_data["title"] == valid_news_data["title"]

        # Test invalid data
        invalid_data = {"news_id": "123"}  # Missing required fields
        with pytest.raises(Exception):
            producer._validate_news_data(invalid_data)

    @pytest.mark.asyncio
    async def test_fetch_news_data(self, producer, valid_news_data):
        """Test news data fetching."""
        producer.news_connectors[
            "newsapi"
        ].fetch_and_publish_news.return_value = valid_news_data

        # Patch the validation method to return the same data
        with patch.object(
            producer, "_validate_news_data", return_value=valid_news_data
        ):
            result = await producer.fetch_news_data("test query")

            assert result == valid_news_data
            producer.news_connectors[
                "newsapi"
            ].fetch_and_publish_news.assert_called_once_with("test query")

    @pytest.mark.asyncio
    async def test_fetch_game_data_success(self, producer, valid_game_data):
        """Test successful game data fetching."""
        # Mock dependencies
        producer.health_handler.can_use_source.return_value = True
        producer.espn_connector.fetch_and_publish_games.return_value = valid_game_data

        # Mock validation
        with patch.object(
            producer, "_validate_game_data", return_value=valid_game_data
        ):
            result = await producer.fetch_game_data(["football"])

            assert result == valid_game_data
            producer.espn_connector.fetch_and_publish_games.assert_called_once()
            producer.health_handler.update_source_status.assert_called_with(
                "espn", True
            )
            if producer.config.enable_metrics:
                producer.metrics.record_message.assert_called_with(
                    "espn", True, pytest.approx(0, abs=1)
                )

    @pytest.mark.asyncio
    async def test_fetch_game_data_failure(self, producer):
        """Test game data fetching with failure."""
        # Mock dependencies
        producer.health_handler.can_use_source.return_value = True
        producer.espn_connector.fetch_and_publish_games.side_effect = Exception(
            "API Error"
        )

        result = await producer.fetch_game_data(["football"])

        assert result is None
        producer.error_handler.handle_error.assert_called_once()
        producer.health_handler.update_source_status.assert_called_with(
            "espn", False, ANY, ANY
        )
        if producer.config.enable_metrics:
            producer.metrics.record_message.assert_called_with("espn", False, 0)

    @pytest.mark.asyncio
    async def test_fetch_game_data_source_unavailable(self, producer):
        """Test game data fetching when source is unavailable."""
        producer.health_handler.can_use_source.return_value = False

        result = await producer.fetch_game_data(["football"])

        assert result is None
        producer.espn_connector.fetch_and_publish_games.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_odds_data_success(self, producer, valid_odds_data):
        """Test successful odds data fetching."""
        # Mock dependencies
        producer.health_handler.can_use_source.return_value = True
        producer.odds_api_connector.fetch_and_publish_odds.return_value = (
            valid_odds_data
        )

        # Mock validation
        with patch.object(
            producer, "_validate_odds_data", return_value=valid_odds_data
        ):
            result = await producer.fetch_odds_data(["football"], ["h2h"])

            assert result == valid_odds_data
            producer.odds_api_connector.fetch_and_publish_odds.assert_called()
            producer.health_handler.update_source_status.assert_called_with(
                "odds_api", True
            )
            if producer.config.enable_metrics:
                producer.metrics.record_message.assert_called_with(
                    "odds_api", True, pytest.approx(0, abs=1)
                )

    @pytest.mark.asyncio
    async def test_fetch_odds_data_failure(self, producer):
        """Test odds data fetching with failure."""
        # Mock dependencies
        producer.health_handler.can_use_source.return_value = True
        producer.odds_api_connector.fetch_and_publish_odds.side_effect = Exception(
            "API Error"
        )

        result = await producer.fetch_odds_data(["football"], ["h2h"])

        assert result is None
        producer.error_handler.handle_error.assert_called_once()
        producer.health_handler.update_source_status.assert_called_with(
            "odds_api", False, ANY, ANY
        )
        if producer.config.enable_metrics:
            producer.metrics.record_message.assert_called_with("odds_api", False, 0)

    @pytest.mark.asyncio
    async def test_fetch_odds_data_source_unavailable(self, producer):
        """Test odds data fetching when source is unavailable."""
        producer.health_handler.can_use_source.return_value = False

        result = await producer.fetch_odds_data(["football"], ["h2h"])

        assert result is None
        producer.odds_api_connector.fetch_and_publish_odds.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_streaming_integration(self, producer):
        """Test the entire streaming pipeline."""
        sports = ["football"]
        markets = ["h2h"]

        # Mock all fetch methods
        with patch.object(producer, "fetch_game_data") as mock_game, patch.object(
            producer, "fetch_odds_data"
        ) as mock_odds, patch.object(
            producer, "fetch_weather_data"
        ) as mock_weather, patch.object(producer, "fetch_news_data") as mock_news:
            # Create a task for streaming
            producer.running = True

            async def stop_streaming():
                await asyncio.sleep(0.2)
                producer.running = False

            # Run streaming briefly
            await asyncio.gather(
                producer.start_streaming(sports, markets), stop_streaming()
            )

            # Verify all fetch methods were called
            mock_game.assert_called_with(sports)
            mock_odds.assert_called_with(sports, markets)
            mock_weather.assert_called()
            mock_news.assert_called()

    @pytest.mark.asyncio
    async def test_start_streaming_integration(self, producer):
        """Test the entire streaming pipeline."""
        sports = ["football"]
        markets = ["h2h"]

        # Mock all fetch methods to return immediately
        producer.fetch_game_data = AsyncMock(return_value={"status": "success"})
        producer.fetch_odds_data = AsyncMock(return_value={"status": "success"})
        producer.fetch_weather_data = AsyncMock(return_value={"status": "success"})
        producer.fetch_news_data = AsyncMock(return_value={"status": "success"})

        # Create a task for streaming with timeout
        try:
            async with asyncio.timeout(1):  # 1 second timeout
                # Start streaming
                producer.running = True

                # Create tasks
                streaming_task = asyncio.create_task(
                    producer.start_streaming(sports, markets)
                )

                # Let it run briefly
                await asyncio.sleep(0.2)

                # Stop the streaming
                producer.running = False

                # Wait for streaming to complete or timeout
                await streaming_task

        except TimeoutError:
            # Ensure we stop even if timeout occurs
            producer.running = False

        # Verify calls
        producer.fetch_game_data.assert_called_with(sports)
        producer.fetch_odds_data.assert_called_with(sports, markets)
        producer.fetch_weather_data.assert_called()
        producer.fetch_news_data.assert_called()

    @pytest.mark.asyncio
    async def test_start_streaming_error_handling(self, producer, caplog):
        """Test error handling in streaming pipeline."""
        sports = ["football"]
        markets = ["h2h"]

        # Mock fetch_game_data to raise an exception
        producer.fetch_game_data = AsyncMock(side_effect=Exception("Test error"))
        producer.fetch_odds_data = AsyncMock(return_value={"status": "success"})
        producer.fetch_weather_data = AsyncMock(return_value={"status": "success"})
        producer.fetch_news_data = AsyncMock(return_value={"status": "success"})

        # Create a task for streaming with timeout
        try:
            async with asyncio.timeout(1):  # 1 second timeout
                # Start streaming
                producer.running = True

                # Create tasks
                streaming_task = asyncio.create_task(
                    producer.start_streaming(sports, markets)
                )

                # Let it run briefly
                await asyncio.sleep(0.2)

                # Stop the streaming
                producer.running = False

                # Wait for streaming to complete or timeout
                await streaming_task

        except TimeoutError:
            # Ensure we stop even if timeout occurs
            producer.running = False

        # Verify error handling
        assert "Test error" in caplog.text
        producer.fetch_game_data.assert_called()

    # @pytest.mark.asyncio
    # async def test_start_streaming(self, producer):
    #     """Test streaming pipeline."""
    #     sports = ["basketball"]
    #     markets = ["h2h"]
    #
    #     # Mock responses
    #     producer.espn_connector.fetch_and_publish_games.return_value = {
    #         "status": "success"
    #     }
    #     producer.odds_api_connector.fetch_and_publish_odds.return_value = {
    #         "status": "success"
    #     }
    #     producer._get_venues_for_sports = AsyncMock(return_value=[])
    #     producer.health_handler.can_use_source.return_value = True
    #
    #     # Create an event for controlled stopping
    #     stop_event = asyncio.Event()
    #
    #     async def run_streaming():
    #         try:
    #             producer.running = True
    #             await producer.start_streaming(sports, markets)
    #         except Exception as e:
    #             print(f"Streaming error: {e}")
    #
    #     async def stop_streaming():
    #         await asyncio.sleep(0.2)
    #         producer.running = False
    #         stop_event.set()
    #
    #     # Run with timeout
    #     try:
    #         async with asyncio.timeout(1):
    #             await asyncio.gather(run_streaming(), stop_streaming())
    #     except asyncio.TimeoutError:
    #         producer.running = False
    #         stop_event.set()
    #
    #     # Verify calls
    #     assert producer.espn_connector.fetch_and_publish_games.called
    #     assert producer.odds_api_connector.fetch_and_publish_odds.called

    def test_get_league_for_sport(self, producer):
        """Test sport to league mapping."""
        assert producer._get_league_for_sport("basketball") == "nba"
        assert producer._get_league_for_sport("football") == "nfl"
        assert producer._get_league_for_sport("baseball") == "mlb"
        assert producer._get_league_for_sport("unknown") is None

    def test_cleanup(self, producer):
        """Test cleanup process."""
        producer.cleanup()
        producer.admin_manager.close.assert_called_once()
