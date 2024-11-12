from dataclasses import dataclass, field
from typing import List
from betflow.kafka_orch.config.topic_config import TopicConfig


@dataclass
class APIConfig:
    api_key: str
    base_url: str
    rate_limit: int
    timeout: int = 30


@dataclass
class ProducerConfig:
    # Kafka Configuration
    bootstrap_servers: str
    client_id: str

    # API Keys
    odds_api_key: str
    openweather_api_key: str
    newsapi_key: str
    gnews_api_key: str
    openmeteo_enabled: bool = True

    # RSS Feed Configuration
    rss_feeds: List[str] = field(default_factory=list)

    # Kafka Producer Settings
    batch_size: int = 16384
    linger_ms: int = 5
    compression_type: str = 'snappy'
    acks: str = 'all'
    retries: int = 3
    max_in_flight_requests_per_connection: int = 5

    # Refresh Intervals
    game_refresh_interval: int = 30
    odds_refresh_interval: int = 60
    weather_refresh_interval: int = 300
    news_refresh_interval: int = 600

    # Topic Configurations
    topics: List['TopicConfig'] = field(default_factory=list)

    # Error Handling
    max_retries: int = 3
    retry_backoff_ms: int = 1000

    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 8000


def create_development_config() -> ProducerConfig:
    pass

# Development config implementation...

def create_production_config() -> ProducerConfig:
    pass
# Production config implementation...