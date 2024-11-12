import logging
import asyncio
import time
from typing import Dict, Optional, Any, List

from betflow.kafka_orch.config.producer_config import ProducerConfig
from betflow.kafka_orch.core.admin import KafkaAdminManager
from betflow.kafka_orch.handlers import KafkaErrorHandler, HealthHandler
from betflow.kafka_orch.monitoring.metrics import MetricsCollector

from betflow.api_connectors import OddsAPIConnector, ESPNConnector, OpenWeatherConnector, OpenMeteoConnector, NewsAPIConnector, GNewsConnector, RSSFeedConnector

class SportsBettingProducer:
    """Unified producer with fallback mechanisms for sports betting data streams."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize core components
        self.admin_manager = KafkaAdminManager(config)
        self.error_handler = KafkaErrorHandler()
        self.health_handler = HealthHandler()

        if config.enable_metrics:
            self.metrics = MetricsCollector(metrics_interval=60)

        # Ensure topics exist
        self.admin_manager.ensure_topics_exist(self.config.topics)

        # Initialize connectors
        self._initialize_connectors()

    def _initialize_connectors(self):
        """Initialize all data source connectors with error handling."""
        # Game Stats Connector
        self.espn_connector = self._init_connector(
            'espn',
            lambda: ESPNConnector(self.config.bootstrap_servers)
        )

        # Odds Connector
        self.odds_api_connector = self._init_connector(
            'odds_api',
            lambda: OddsAPIConnector(
                api_key=self.config.odds_api_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers
            )
        )

        # Weather Connectors
        self.weather_connectors = {
            'openweather': self._init_connector(
                'openweather',
                lambda: OpenWeatherConnector(
                    api_key=self.config.openweather_api_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers
                )
            ),
            'openmeteo': self._init_connector(
                'openmeteo',
                lambda: OpenMeteoConnector(
                    kafka_bootstrap_servers=self.config.bootstrap_servers
                )
            )
        }

        # News Connectors
        self.news_connectors = {
            'newsapi': self._init_connector(
                'newsapi',
                lambda: NewsAPIConnector(
                    api_key=self.config.newsapi_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers
                )
            ),
            'gnews': self._init_connector(
                'gnews',
                lambda: GNewsConnector(
                    api_key=self.config.gnews_api_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers
                )
            ),
            'rss': self._init_connector(
                'rss',
                lambda: RSSFeedConnector(
                    kafka_bootstrap_servers=self.config.bootstrap_servers,
                    feed_urls=self.config.rss_feeds
                )
            )
        }

    def _init_connector(self, name: str, init_func: callable) -> Optional[Any]:
        """Initialize a connector with error handling."""
        try:
            connector = init_func()
            self.logger.info(f"{name} connector initialized successfully")
            self.health_handler.initialize_source(name)
            return connector
        except Exception as e:
            self.logger.error(f"Failed to initialize {name} connector: {e}")
            self.health_handler.mark_source_failed(name)
            return None

    async def fetch_weather_data(self, venue: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch weather data with fallback mechanism."""
        weather_sources = ['openweather', 'openmeteo']

        for source in weather_sources:
            if self.health_handler.can_use_source(source):
                try:
                    start_time = time.time()

                    if source == 'openweather':
                        data = await self.weather_connectors['openweather'].fetch_and_publish_weather(
                            venue['city']
                        )
                    else:
                        data = await self.weather_connectors['openmeteo'].fetch_and_publish_forecast(
                            latitude=venue['lat'],
                            longitude=venue['lon']
                        )

                    latency = time.time() - start_time
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, True, latency)

                    self.health_handler.update_source_status(source, True)
                    return data

                except Exception as e:
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, False, 0)

                    retry_after = self.error_handler.handle_error(e, source)
                    self.health_handler.update_source_status(source, False, e, retry_after)
                    continue

        self.logger.error("All weather data sources failed")
        return None

    async def fetch_news_data(self, query: str) -> Optional[Dict[str, Any]]:
        """Fetch news data with fallback mechanism."""
        news_sources = ['newsapi', 'gnews', 'rss']

        for source in news_sources:
            if self.health_handler.can_use_source(source):
                try:
                    start_time = time.time()

                    if source == 'newsapi':
                        data = await self.news_connectors['newsapi'].fetch_and_publish_news(query)
                    elif source == 'gnews':
                        data = await self.news_connectors['gnews'].fetch_and_publish_news(query)
                    else:
                        data = await self.news_connectors['rss'].fetch_and_publish_feeds()

                    latency = time.time() - start_time
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, True, latency)

                    self.health_handler.update_source_status(source, True)
                    return data

                except Exception as e:
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, False, 0)

                    retry_after = self.error_handler.handle_error(e, source)
                    self.health_handler.update_source_status(source, False, e, retry_after)
                    continue

        self.logger.error("All news data sources failed")
        return None

    async def start_streaming(self, sports: List[str], markets: List[str]):
        """Start streaming data from all sources."""
        while True:
            try:
                # Fetch game data
                if self.health_handler.can_use_source('espn'):
                    try:
                        start_time = time.time()
                        for sport in sports:
                            await self.espn_connector.fetch_and_publish_games(
                                sport=sport,
                                league=self._get_league_for_sport(sport)
                            )

                        if self.config.enable_metrics:
                            self.metrics.record_message(
                                'espn',
                                True,
                                time.time() - start_time
                            )

                        self.health_handler.update_source_status('espn', True)
                    except Exception as e:
                        if self.config.enable_metrics:
                            self.metrics.record_message('espn', False, 0)

                        retry_after = self.error_handler.handle_error(e, 'espn')
                        self.health_handler.update_source_status('espn', False, e, retry_after)
                
                # fetch odds data
                if self.health_handler.can_use_source('odds_api'):
                    try:
                        start_time = time.time()
                        for sport in sports:
                            for market in markets:
                                await self.odds_api_connector.fetch_and_publish_odds(
                                    sport=sport,
                                    markets=market
                                )
                        if self.config.enable_metrics:
                            self.metrics.record_message(
                                'odds_api',
                                True,
                                time.time() - start_time
                            )
                        self.health_handler.update_source_status('odds_api', True)
                    except Exception as e:
                        if self.config.enable_metrics:
                            self.metrics.record_message('odds_api', False, 0)

                        retry_after = self.error_handler.handle_error(e, 'odds_api')
                        self.health_handler.update_source_status('odds_api', False, e, retry_after)

                # Fetch weather data
                venues = await self._get_venues_for_sports(sports)
                for venue in venues:
                    await self.fetch_weather_data(venue)
                    
                # Fetch news data
                for sport in sports:
                    await self.fetch_news_data(f"{sport} betting")
    
                # Log metrics if enabled
                if self.config.enable_metrics:
                    self.metrics.log_metrics()
    
                # Wait before next iteration
                await asyncio.sleep(self.config.refresh_interval)

            except Exception as e:
                self.logger.error(f"Error in streaming pipeline: {e}")
                await asyncio.sleep(5)

    def cleanup(self):
        """Clean up resources."""
        try:
            # Close connectors
            for connector in [self.espn_connector, self.odds_api_connector]:
                if connector:
                    connector.close()

            for weather_connector in self.weather_connectors.values():
                if weather_connector:
                    weather_connector.close()

            for news_connector in self.news_connectors.values():
                if news_connector:
                    news_connector.close()

            # Close admin client
            self.admin_manager.close()

            # Final metrics log
            if self.config.enable_metrics:
                self.metrics.log_metrics()

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    @staticmethod
    def _get_league_for_sport(sport: str) -> str:
        """Map sport to league."""
        sport_league_mapping = {
            'basketball': 'nba',
            'football': 'nfl',
            'baseball': 'mlb',
            # 'hockey': 'nhl'
        }
        return sport_league_mapping.get(sport)