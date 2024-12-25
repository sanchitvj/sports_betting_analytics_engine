import logging
import asyncio
import time
from typing import Dict, Optional, Any, List

from betflow.kafka_orch.config.producer_config import ProducerConfig
from betflow.kafka_orch.core.admin import KafkaAdminManager
from betflow.kafka_orch.handlers import KafkaErrorHandler, HealthHandler
from betflow.kafka_orch.monitoring.metrics import MetricsCollector
from betflow.kafka_orch.schemas import (
    NFLGameStats,
    NBAGameStats,
    MLBGameStats,
    OddsData,
    WeatherData,
    NewsData,
)

from betflow.api_connectors import (
    OddsAPIConnector,
    ESPNConnector,
    OpenWeatherConnector,
    OpenMeteoConnector,
    NewsAPIConnector,
    GNewsConnector,
    RSSFeedConnector,
)


class SportsBettingProducer:
    """Unified producer with fallback mechanisms for sports analytics data streams."""

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

    def _validate_game_data(self, sport: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate game data against appropriate schema."""
        try:
            if sport == "football":
                return NFLGameStats(
                    **data
                ).model_dump()  # Changed from dict() to model_dump()
            elif sport == "basketball":
                return NBAGameStats(**data).model_dump()
            elif sport == "baseball":
                return MLBGameStats(**data).model_dump()
            else:
                raise ValueError(f"Unsupported sport: {sport}")
        except Exception as e:
            self.logger.error(f"Game data validation failed for {sport}: {e}")
            raise

    def _validate_weather_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate weather data against schema."""
        try:
            return WeatherData(
                **data
            ).model_dump()  # Changed from dict() to model_dump()
        except Exception as e:
            self.logger.error(f"Weather data validation failed: {e}")
            raise

    def _validate_news_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate news data against schema."""
        try:
            return NewsData(**data).model_dump()  # Changed from dict() to model_dump()
        except Exception as e:
            self.logger.error(f"News data validation failed: {e}")
            raise

    def _validate_odds_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate odds data against schema."""
        try:
            return OddsData(**data).model_dump()  # Changed from dict() to model_dump()
        except Exception as e:
            self.logger.error(f"Odds data validation failed: {e}")
            raise

    def _initialize_connectors(self):
        """Initialize all data source connectors with error handling."""
        # Initialize connectors with None first
        self.espn_connector = None
        self.odds_api_connector = None
        self.weather_connectors = {}
        self.news_connectors = {}

        # Game Stats Connector
        self.espn_connector = self._init_connector(
            "espn", lambda: ESPNConnector(self.config.bootstrap_servers)
        )

        # Odds Connector
        self.odds_api_connector = self._init_connector(
            "odds_api",
            lambda: OddsAPIConnector(
                api_key=self.config.odds_api_key,
                kafka_bootstrap_servers=self.config.bootstrap_servers,
            ),
        )

        # Weather Connectors
        self.weather_connectors = {
            "openweather": self._init_connector(
                "openweather",
                lambda: OpenWeatherConnector(
                    api_key=self.config.openweather_api_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers,
                ),
            ),
            "openmeteo": self._init_connector(
                "openmeteo",
                lambda: OpenMeteoConnector(
                    kafka_bootstrap_servers=self.config.bootstrap_servers
                ),
            ),
        }

        # News Connectors
        self.news_connectors = {
            "newsapi": self._init_connector(
                "newsapi",
                lambda: NewsAPIConnector(
                    api_key=self.config.newsapi_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers,
                ),
            ),
            "gnews": self._init_connector(
                "gnews",
                lambda: GNewsConnector(
                    api_key=self.config.gnews_api_key,
                    kafka_bootstrap_servers=self.config.bootstrap_servers,
                ),
            ),
            "rss": self._init_connector(
                "rss",
                lambda: RSSFeedConnector(
                    kafka_bootstrap_servers=self.config.bootstrap_servers,
                    feed_urls=self.config.rss_feeds,
                ),
            ),
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

    async def fetch_weather_data(
        self, venue: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Fetch weather data with fallback mechanism."""
        weather_sources = ["openweather", "openmeteo"]

        for source in weather_sources:
            if self.health_handler.can_use_source(source):
                try:
                    start_time = time.time()

                    if source == "openweather" and self.weather_connectors.get(
                        "openweather"
                    ):
                        raw_data = await self.weather_connectors[
                            "openweather"
                        ].fetch_and_publish_weather(venue["city"])
                    else:  # self.weather_connectors.get("openmeteo"):
                        raw_data = await self.weather_connectors[
                            "openmeteo"
                        ].fetch_and_publish_forecast(
                            latitude=venue["lat"], longitude=venue["lon"]
                        )
                    # else:
                    #     continue
                    data = self._validate_weather_data(raw_data)

                    latency = time.time() - start_time
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, True, latency)

                    self.health_handler.update_source_status(source, True)
                    return data

                except Exception as e:
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, False, 0)

                    retry_after = self.error_handler.handle_error(e, source)
                    self.health_handler.update_source_status(
                        source, False, e, retry_after
                    )
                    continue

        self.logger.error("All weather data sources failed")
        return None

    async def fetch_news_data(self, query: str) -> Optional[Dict[str, Any]]:
        """Fetch news data with fallback mechanism."""
        news_sources = ["newsapi", "gnews", "rss"]

        for source in news_sources:
            if self.health_handler.can_use_source(source):
                try:
                    start_time = time.time()

                    if source == "newsapi":
                        raw_data = await self.news_connectors[
                            "newsapi"
                        ].fetch_and_publish_news(query)
                    elif source == "gnews":
                        raw_data = await self.news_connectors[
                            "gnews"
                        ].fetch_and_publish_news(query)
                    else:
                        raw_data = await self.news_connectors[
                            "rss"
                        ].fetch_and_publish_feeds()

                    data = self._validate_news_data(raw_data)

                    latency = time.time() - start_time
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, True, latency)

                    self.health_handler.update_source_status(source, True)
                    return data

                except Exception as e:
                    if self.config.enable_metrics:
                        self.metrics.record_message(source, False, 0)

                    retry_after = self.error_handler.handle_error(e, source)
                    self.health_handler.update_source_status(
                        source, False, e, retry_after
                    )
                    continue

        self.logger.error("All news data sources failed")
        return None

    async def fetch_game_data(self, sports: List[str]) -> Optional[Dict[str, Any]]:
        """Fetch game data with error handling."""
        if self.health_handler.can_use_source("espn"):
            try:
                start_time = time.time()
                for sport in sports:
                    raw_data = await self.espn_connector.fetch_and_publish_games(
                        sport=sport, league=self._get_league_for_sport(sport)
                    )
                    data = self._validate_game_data(sport, raw_data)

                latency = time.time() - start_time
                if self.config.enable_metrics:
                    self.metrics.record_message("espn", True, latency)

                self.health_handler.update_source_status("espn", True)
                return data

            except Exception as e:
                if self.config.enable_metrics:
                    self.metrics.record_message("espn", False, 0)

                retry_after = self.error_handler.handle_error(e, "espn")
                self.health_handler.update_source_status("espn", False, e, retry_after)
                return None

    async def fetch_odds_data(
        self, sports: List[str], markets: List[str]
    ) -> Optional[Dict[str, Any]]:
        """Fetch odds data with error handling."""
        if self.health_handler.can_use_source("odds_api"):
            try:
                start_time = time.time()
                for sport in sports:
                    for market in markets:
                        raw_data = await self.odds_api_connector.fetch_and_publish_odds(
                            sport=sport, markets=market
                        )
                        data = self._validate_odds_data(raw_data)

                latency = time.time() - start_time
                if self.config.enable_metrics:
                    self.metrics.record_message("odds_api", True, latency)

                self.health_handler.update_source_status("odds_api", True)
                return data

            except Exception as e:
                if self.config.enable_metrics:
                    self.metrics.record_message("odds_api", False, 0)

                retry_after = self.error_handler.handle_error(e, "odds_api")
                self.health_handler.update_source_status(
                    "odds_api", False, e, retry_after
                )
                return None

    async def start_streaming(self, sports: List[str], markets: List[str]):
        """Start streaming data from all sources."""
        while True:
            try:
                # Fetch all data types
                await self.fetch_game_data(sports)
                await self.fetch_odds_data(sports, markets)

                venues = await self._get_venues_for_sports(sports)
                for venue in venues:
                    await self.fetch_weather_data(venue)

                for sport in sports:
                    await self.fetch_news_data(f"{sport} analytics")

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
            "basketball": "nba",
            "football": "nfl",
            "baseball": "mlb",
            # 'hockey': 'nhl'
        }
        return sport_league_mapping.get(sport)
