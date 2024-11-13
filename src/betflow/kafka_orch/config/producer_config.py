from dataclasses import dataclass, field
from typing import List


@dataclass
class ProducerConfig:
    # Required fields
    bootstrap_servers: str
    client_id: str
    odds_api_key: str
    openweather_api_key: str
    newsapi_key: str
    gnews_api_key: str

    # Optional fields with defaults
    rss_feeds: List[str] = field(default_factory=list)
    batch_size: int = 16384
    linger_ms: int = 5
    compression_type: str = "snappy"
    acks: str = "all"
    retries: int = 3
    max_in_flight_requests_per_connection: int = 5

    # Refresh intervals
    game_refresh_interval: int = 30
    odds_refresh_interval: int = 60
    weather_refresh_interval: int = 300
    news_refresh_interval: int = 600

    # Error handling
    max_retries: int = 3
    retry_backoff_ms: int = 1000

    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 8000

    def __post_init__(self):
        if not self.bootstrap_servers:
            raise ValueError("bootstrap_servers cannot be empty")


def create_development_config() -> ProducerConfig:
    """Create development environment configuration."""
    return ProducerConfig(
        bootstrap_servers="localhost:9092",
        client_id="sports_betting_producer_dev",
        odds_api_key="dev_odds_key",
        openweather_api_key="dev_weather_key",
        newsapi_key="dev_news_key",
        gnews_api_key="dev_gnews_key",
        rss_feeds=["http://example.com/feed"],
        batch_size=16384,
        enable_metrics=True,
    )


def create_production_config() -> ProducerConfig:
    """Create production environment configuration."""
    import os

    return ProducerConfig(
        bootstrap_servers="prod-kafka1:9092,prod-kafka2:9092",
        client_id="sports_betting_producer_prod",
        odds_api_key=os.getenv("ODDS_API_KEY"),
        openweather_api_key=os.getenv("OPENWEATHER_API_KEY"),
        newsapi_key=os.getenv("NEWSAPI_KEY"),
        gnews_api_key=os.getenv("GNEWS_API_KEY"),
        rss_feeds=os.getenv("RSS_FEEDS", "").split(","),
        batch_size=32768,  # Larger for production
        enable_metrics=True,
    )
