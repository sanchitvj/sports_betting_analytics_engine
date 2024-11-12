from enum import Enum
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import logging
from datetime import datetime, timedelta
import asyncio
import requests
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from betflow.api_connectors.odds_conn import OddsAPIConnector
from betflow.api_connectors.games_conn import ESPNConnector
from betflow.api_connectors.weather_conn import OpenWeatherConnector, OpenMeteoConnector
from betflow.api_connectors.news_conn import NewsAPIConnector, GNewsConnector, RSSFeedConnector


class DataCategory(Enum):
    GAMES = "games"
    ODDS = "odds"
    WEATHER = "weather"
    NEWS = "news"


@dataclass
class TopicConfig:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: Optional[int] = None
    cleanup_policy: str = "delete"
    compression_type: str = "gzip"

    @property
    def configs(self) -> Dict[str, str]:
        configs = {
            'cleanup.policy': self.cleanup_policy,
            'compression.type': self.compression_type
        }
        if self.retention_ms:
            configs['retention.ms'] = str(self.retention_ms)
        return configs


@dataclass
class APIConfig:
    api_key: str
    base_url: str
    rate_limit: int  # requests per minute
    timeout: int = 30  # seconds


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
    openmeteo_enabled: bool = True  # No API key needed

    # RSS Feed Configuration
    rss_feeds: List[str] = field(default_factory=list)

    # Kafka Producer Settings
    batch_size: int = 16384
    linger_ms: int = 5
    compression_type: str = 'snappy'
    acks: str = 'all'
    retries: int = 3
    max_in_flight_requests_per_connection: int = 5

    # Refresh Intervals (seconds)
    game_refresh_interval: int = 30
    odds_refresh_interval: int = 60
    weather_refresh_interval: int = 300  # 5 minutes
    news_refresh_interval: int = 600  # 10 minutes

    # Topic Configurations
    topics: List[TopicConfig] = field(default_factory=list)

    # Error Handling
    max_retries: int = 3
    retry_backoff_ms: int = 1000

    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 8000
    
class DataSourceStatus(Enum):
    ACTIVE = "active"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"


@dataclass
class DataSourceHealth:
    status: DataSourceStatus
    last_success: Optional[datetime] = None
    failure_count: int = 0
    next_retry: Optional[datetime] = None


class SportsBettingProducer:
    """Unified producer with fallback mechanisms for sports betting data streams."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.admin_client = self._create_admin_client()
        self._ensure_topics_exist()

        # Initialize all connectors
        self._initialize_connectors()

        # Initialize health tracking
        self.health_status = self._initialize_health_status()

    def _create_admin_client(self) -> KafkaAdminClient:
        """Create Kafka admin client for topic management."""
        try:
            return KafkaAdminClient(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=f"{self.config.client_id}_admin",
                request_timeout_ms=30000,  # 30 seconds timeout
                connections_max_idle_ms=300000  # 5 minutes idle timeout
            )
        except Exception as e:
            self.logger.error(f"Failed to create admin client: {e}")
            raise

    def _ensure_topics_exist(self) -> None:
        """Ensure all required Kafka topics exist with proper configurations."""
        try:
            existing_topics = self.admin_client.list_topics()
            topics_to_create = []

            for topic_config in self.config.topics:
                if topic_config.name not in existing_topics:
                    new_topic = NewTopic(
                        name=topic_config.name,
                        num_partitions=topic_config.partitions,
                        replication_factor=topic_config.replication_factor,
                        topic_configs=topic_config.configs
                    )
                    topics_to_create.append(new_topic)

            if topics_to_create:
                try:
                    self.admin_client.create_topics(
                        new_topics=topics_to_create,
                        validate_only=False
                    )
                    self.logger.info(f"Created {len(topics_to_create)} new topics")
                except TopicAlreadyExistsError:
                    self.logger.warning("Some topics already exist")
                except Exception as e:
                    self.logger.error(f"Failed to create topics: {e}")
                    raise

        except Exception as e:
            self.logger.error(f"Error ensuring topics exist: {e}")
            raise
        
    def _initialize_connectors(self):
        """Initialize all data source connectors."""
        try:
            # Game Stats Connectors
            self.espn_connector = ESPNConnector(self.config.bootstrap_servers)
            self.logger.info("ESPN connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize ESPN connector: {e}")
            self.health_status['espn'] = DataSourceHealth(DataSourceStatus.FAILED)

        try:
            # Odds Connectors
            self.odds_api_connector = OddsAPIConnector(
                api_key=self.config.odds_api_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
            self.logger.info("Odds API connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Odds API connector: {e}")
            self.health_status['odds_api'] = DataSourceHealth(DataSourceStatus.FAILED)

        # Weather Connectors with Fallback
        self.weather_connectors = {
            'openweather': None,
            'openmeteo': None
        }

        try:
            self.weather_connectors['openweather'] = OpenWeatherConnector(
                api_key=self.config.openweather_api_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
            self.logger.info("OpenWeather connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenWeather connector: {e}")
            self.health_status['openweather'] = DataSourceHealth(DataSourceStatus.FAILED)

        try:
            self.weather_connectors['openmeteo'] = OpenMeteoConnector(
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
            self.logger.info("OpenMeteo connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenMeteo connector: {e}")
            self.health_status['openmeteo'] = DataSourceHealth(DataSourceStatus.FAILED)

        # News Connectors with Fallback
        self.news_connectors = {
            'newsapi': None,
            'gnews': None,
            'rss': None
        }

        try:
            self.news_connectors['newsapi'] = NewsAPIConnector(
                api_key=self.config.newsapi_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
            self.logger.info("NewsAPI connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize NewsAPI connector: {e}")
            self.health_status['newsapi'] = DataSourceHealth(DataSourceStatus.FAILED)

        try:
            self.news_connectors['gnews'] = GNewsConnector(
                api_key=self.config.gnews_api_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
            self.logger.info("GNews connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize GNews connector: {e}")
            self.health_status['gnews'] = DataSourceHealth(DataSourceStatus.FAILED)

        try:
            self.news_connectors['rss'] = RSSFeedConnector(
                kafka_bootstrap_servers=self.config.bootstrap_servers,
                feed_urls=self.config.rss_feeds
            )
            self.logger.info("RSS connector initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize RSS connector: {e}")
            self.health_status['rss'] = DataSourceHealth(DataSourceStatus.FAILED)
    
    @staticmethod
    def _initialize_health_status() -> Dict[str, DataSourceHealth]:
        """Initialize health status tracking for all data sources."""
        return {
            'espn': DataSourceHealth(DataSourceStatus.ACTIVE),
            'odds_api': DataSourceHealth(DataSourceStatus.ACTIVE),
            'openweather': DataSourceHealth(DataSourceStatus.ACTIVE),
            'openmeteo': DataSourceHealth(DataSourceStatus.ACTIVE),
            'newsapi': DataSourceHealth(DataSourceStatus.ACTIVE),
            'gnews': DataSourceHealth(DataSourceStatus.ACTIVE),
            'rss': DataSourceHealth(DataSourceStatus.ACTIVE)
        }

    def _update_health_status(self, source: str, success: bool, error: Optional[Exception] = None):
        """Update health status for a data source."""
        health = self.health_status[source]

        if success:
            health.status = DataSourceStatus.ACTIVE
            health.last_success = datetime.now()
            health.failure_count = 0
            health.next_retry = None
        else:
            health.failure_count += 1
            if isinstance(error, requests.exceptions.HTTPError) and error.response.status_code == 429:
                health.status = DataSourceStatus.RATE_LIMITED
                retry_after = int(error.response.headers.get('Retry-After', 3600))
                health.next_retry = datetime.now() + timedelta(seconds=retry_after)
            else:
                health.status = DataSourceStatus.FAILED
                # Exponential backoff for retries
                retry_delay = min(300, 2 ** health.failure_count)  # Max 5 minutes
                health.next_retry = datetime.now() + timedelta(seconds=retry_delay)

    async def fetch_weather_data(self, venue: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch weather data with fallback mechanism."""
        weather_sources = ['openweather', 'openmeteo']

        for source in weather_sources:
            if (self.health_status[source].status != DataSourceStatus.FAILED and
                (self.health_status[source].next_retry is None or
                 datetime.now() >= self.health_status[source].next_retry)):
                try:
                    if source == 'openweather':
                        data = await self.weather_connectors['openweather'].fetch_and_publish_weather(venue['city'])
                    else:
                        data = await self.weather_connectors['openmeteo'].fetch_and_publish_forecast(
                            latitude=venue['lat'],
                            longitude=venue['lon']
                        )
                    self._update_health_status(source, True)
                    return data
                except Exception as e:
                    self.logger.error(f"Error fetching weather from {source}: {e}")
                    self._update_health_status(source, False, e)
                    continue

        self.logger.error("All weather data sources failed")
        return None

    async def fetch_news_data(self, query: str) -> Optional[Dict[str, Any]]:
        """Fetch news data with fallback mechanism."""
        news_sources = ['newsapi', 'gnews', 'rss']

        for source in news_sources:
            if (self.health_status[source].status != DataSourceStatus.FAILED and
                (self.health_status[source].next_retry is None or
                 datetime.now() >= self.health_status[source].next_retry)):
                try:
                    if source == 'newsapi':
                        data = await self.news_connectors['newsapi'].fetch_and_publish_news(query)
                    elif source == 'gnews':
                        data = await self.news_connectors['gnews'].fetch_and_publish_news(query)
                    else:
                        data = await self.news_connectors['rss'].fetch_and_publish_feeds()
                    self._update_health_status(source, True)
                    return data
                except Exception as e:
                    self.logger.error(f"Error fetching news from {source}: {e}")
                    self._update_health_status(source, False, e)
                    continue

        self.logger.error("All news data sources failed")
        return None

    async def start_streaming(self, sports: List[str], markets: List[str]):
        """Start streaming data from all sources with fallback mechanisms."""
        while True:
            try:
                # Fetch game data
                if self.health_status['espn'].status != DataSourceStatus.FAILED:
                    try:
                        for sport in sports:
                            await self.espn_connector.fetch_and_publish_games(
                                sport=sport,
                                league=self._get_league_for_sport(sport)
                            )
                        self._update_health_status('espn', True)
                    except Exception as e:
                        self.logger.error(f"Error fetching ESPN data: {e}")
                        self._update_health_status('espn', False, e)

                # Fetch odds data
                if self.health_status['odds_api'].status != DataSourceStatus.FAILED:
                    try:
                        for sport in sports:
                            for market in markets:
                                await self.odds_api_connector.fetch_and_publish_odds(
                                    sport=sport,
                                    markets=market
                                )
                        self._update_health_status('odds_api', True)
                    except Exception as e:
                        self.logger.error(f"Error fetching odds data: {e}")
                        self._update_health_status('odds_api', False, e)

                # Fetch weather data for venues
                venues = await self._get_venues_for_sports(sports)
                for venue in venues:
                    await self.fetch_weather_data(venue)

                # Fetch news data
                for sport in sports:
                    await self.fetch_news_data(f"{sport} betting")

                # Wait before next iteration
                await asyncio.sleep(self.config.refresh_interval)

            except Exception as e:
                self.logger.error(f"Error in streaming pipeline: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    def close(self):
        """Clean up resources."""
        for connector in [self.espn_connector, self.odds_api_connector]:
            if connector:
                connector.close()

        for weather_connector in self.weather_connectors.values():
            if weather_connector:
                weather_connector.close()

        for news_connector in self.news_connectors.values():
            if news_connector:
                news_connector.close()

        self.admin_client.close()


def create_default_topics() -> List[TopicConfig]:
    """Create default topic configurations for all data categories."""

    # Game Data Topics
    game_topics = [
        TopicConfig(
            name="sports.games.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete"
        ),
        TopicConfig(
            name="sports.games.completed",
            partitions=3,
            replication_factor=1,
            retention_ms=604800000,  # 7 days
            cleanup_policy="compact"
        )
    ]

    # Odds Data Topics
    odds_topics = [
        TopicConfig(
            name="sports.odds.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete"
        ),
        TopicConfig(
            name="sports.odds.history",
            partitions=3,
            replication_factor=1,
            retention_ms=604800000,  # 7 days
            cleanup_policy="compact"
        )
    ]

    # Weather Data Topics
    weather_topics = [
        TopicConfig(
            name="sports.weather.current",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete"
        ),
        TopicConfig(
            name="sports.weather.forecast",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete"
        )
    ]

    # News Data Topics
    news_topics = [
        TopicConfig(
            name="sports.news.live",
            partitions=3,
            replication_factor=1,
            retention_ms=86400000,  # 24 hours
            cleanup_policy="delete"
        ),
        TopicConfig(
            name="sports.news.archive",
            partitions=3,
            replication_factor=1,
            retention_ms=2592000000,  # 30 days
            cleanup_policy="compact"
        )
    ]

    return game_topics + odds_topics + weather_topics + news_topics


def create_development_config() -> ProducerConfig:
    """Create development environment configuration."""
    return ProducerConfig(
        bootstrap_servers='localhost:9092',
        client_id='sports_betting_producer_dev',
        odds_api_key='your_odds_api_key',
        openweather_api_key='your_openweather_key',
        newsapi_key='your_newsapi_key',
        gnews_api_key='your_gnews_key',
        rss_feeds=[
            'http://sports.example.com/feed',
            'http://betting.example.com/feed'
        ],
        topics=create_default_topics(),
        batch_size=16384,
        linger_ms=5,
        compression_type='snappy',
        game_refresh_interval=30,
        odds_refresh_interval=60,
        weather_refresh_interval=300,
        news_refresh_interval=600,
        enable_metrics=True
    )


def create_production_config() -> ProducerConfig:
    """Create production environment configuration."""
    return ProducerConfig(
        bootstrap_servers='prod-kafka1:9092,prod-kafka2:9092,prod-kafka3:9092',
        client_id='sports_betting_producer_prod',
        odds_api_key=os.getenv('ODDS_API_KEY'),
        openweather_api_key=os.getenv('OPENWEATHER_API_KEY'),
        newsapi_key=os.getenv('NEWSAPI_KEY'),
        gnews_api_key=os.getenv('GNEWS_API_KEY'),
        rss_feeds=[
            os.getenv('RSS_FEED_1'),
            os.getenv('RSS_FEED_2')
        ],
        topics=create_default_topics(),
        batch_size=32768,  # Larger batch size for production
        linger_ms=10,
        compression_type='snappy',
        game_refresh_interval=15,  # More frequent updates in production
        odds_refresh_interval=30,
        weather_refresh_interval=300,
        news_refresh_interval=600,
        enable_metrics=True,
        metrics_port=8000
    )


# Usage example:
def main():
    # For development
    config = create_development_config()
    producer = SportsBettingProducer(config)

    # For production
    # config = create_production_config()
    # producer = SportsBettingProducer(config)

    try:
        asyncio.run(producer.start_streaming(
            sports=['basketball', 'football', 'baseball'],
            markets=['h2h', 'spreads', 'totals']
        ))
    except KeyboardInterrupt:
        logging.info("Shutting down producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()