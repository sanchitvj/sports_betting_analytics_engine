import pytest
from unittest.mock import patch
from betflow.kafka_orch.config.producer_config import (
    ProducerConfig,
    create_development_config,
    create_production_config,
)


class TestProducerConfig:
    @pytest.fixture
    def basic_config(self):
        return ProducerConfig(
            bootstrap_servers="localhost:9092",
            client_id="test_client",
            odds_api_key="test_odds_key",
            openweather_api_key="test_weather_key",
            newsapi_key="test_news_key",
            gnews_api_key="test_gnews_key",
        )

    def test_basic_config_initialization(self, basic_config):
        """Test basic configuration initialization."""
        assert basic_config.bootstrap_servers == "localhost:9092"
        assert basic_config.client_id == "test_client"
        assert basic_config.batch_size == 16384  # default value
        assert basic_config.compression_type == "snappy"  # default value

    def test_development_config(self):
        """Test development configuration creation."""
        config = create_development_config()
        assert config.bootstrap_servers == "localhost:9092"
        assert config.batch_size == 16384
        assert config.enable_metrics is True
        assert isinstance(config.odds_api_key, str)
        assert isinstance(config.openweather_api_key, str)
        assert isinstance(config.newsapi_key, str)
        assert isinstance(config.gnews_api_key, str)

    def test_production_config(self):
        """Test production configuration creation."""
        with patch.dict(
            "os.environ",
            {
                "ODDS_API_KEY": "prod_odds_key",
                "OPENWEATHER_API_KEY": "prod_weather_key",
                "NEWSAPI_KEY": "prod_news_key",
                "GNEWS_API_KEY": "prod_gnews_key",
                "RSS_FEEDS": "feed1,feed2",
            },
        ):
            config = create_production_config()
            assert "prod-kafka" in config.bootstrap_servers
            assert config.batch_size == 32768  # Larger for production
            assert config.enable_metrics is True
            assert config.odds_api_key == "prod_odds_key"
            assert len(config.rss_feeds) == 2

    def test_invalid_config(self):
        """Test configuration validation."""
        with pytest.raises(ValueError):
            ProducerConfig(
                bootstrap_servers="",  # Empty bootstrap servers
                client_id="test_client",
                odds_api_key="test_key",
                openweather_api_key="test_weather_key",
                newsapi_key="test_news_key",
                gnews_api_key="test_gnews_key",
            )

    def test_refresh_intervals(self, basic_config):
        """Test refresh interval settings."""
        assert basic_config.game_refresh_interval == 30
        assert basic_config.odds_refresh_interval == 60
        assert basic_config.weather_refresh_interval == 300
        assert basic_config.news_refresh_interval == 600

    def test_error_handling_config(self, basic_config):
        """Test error handling configuration."""
        assert basic_config.max_retries == 3
        assert basic_config.retry_backoff_ms == 1000

    def test_metrics_config(self, basic_config):
        """Test metrics configuration."""
        assert basic_config.enable_metrics is True
        assert basic_config.metrics_port == 8000
