import pytest
from unittest.mock import patch, MagicMock
import requests
from kafka.errors import KafkaTimeoutError
from betflow.api_connectors.odds_conn import OddsAPIConnector


class MockResponse:
    """Mock HTTP response"""

    def __init__(self, json_data, status_code=200, headers=None):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.ok = status_code < 400

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if not self.ok:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} Error", response=self
            )


@pytest.fixture(autouse=True)
def mock_kafka_producer():
    """Mock KafkaProducer at module level"""
    with patch("betflow.api_connectors.odds_conn.KafkaProducer") as mock:
        mock_instance = MagicMock()
        mock_instance.send.return_value.get.return_value = None
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_session():
    """Create mock session with proper configuration"""
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock()
    return session


@pytest.fixture
def connector():
    """Create OddsAPIConnector with mocked dependencies"""
    return OddsAPIConnector(
        api_key="test_api_key", kafka_bootstrap_servers="localhost:9092"
    )


class TestOddsAPIConnector:
    def test_init(self, connector):
        """Test connector initialization"""
        assert connector.base_url == "https://api.the-odds-api.com/v4"
        assert connector.api_key == "test_api_key"
        assert connector.producer is not None

    def test_make_request_success(self, connector):
        """Test successful API request"""
        mock_response = {
            "id": "0137c593a0d5d41469e9",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "commence_time": "2024-11-09T22:00Z",
            "home_team": "San Antonio Spurs",
            "away_team": "Utah Jazz",
            "bookmakers": [
                {
                    "key": "fanduel",
                    "title": "FanDuel",
                    "last_update": "2024-11-09T12:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "San Antonio Spurs", "price": 1.74},
                                {"name": "Utah Jazz", "price": 2.15},
                            ],
                        }
                    ],
                }
            ],
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch("requests.Session", return_value=session_mock):
            connector.session = session_mock
            response = connector.make_request("sports/basketball_nba/odds")

            assert response == mock_response
            session_mock.get.assert_called_once_with(
                f"{connector.base_url}/sports/basketball_nba/odds",
                headers={"apikey": connector.api_key},
                params=None,
                timeout=30,
            )

    def test_make_request_rate_limit_error(self, connector):
        """Test rate limit error handling"""
        session_mock = MagicMock(spec=requests.Session)

        def raise_rate_limit(*args, **kwargs):
            raise requests.exceptions.HTTPError(
                "429 Client Error: Too Many Requests",
                response=MagicMock(status_code=429, headers={"Retry-After": "5"}),
            )

        session_mock.get.side_effect = raise_rate_limit

        with patch("requests.Session", return_value=session_mock):
            connector.session = session_mock

            with pytest.raises(Exception) as exc_info:
                connector.make_request("sports/basketball_nba/odds")
            assert "Rate limit exceeded" in str(exc_info.value)

    def test_transform_odds_data(self, connector):
        """Test odds data transformation"""
        raw_data = {
            "id": "0137c593a0d5d41469e9",
            "sport_key": "basketball_nba",
            "sport_title": "NBA",
            "commence_time": "2024-11-09T22:00Z",
            "home_team": "San Antonio Spurs",
            "away_team": "Utah Jazz",
            "bookmakers": [
                {
                    "key": "fanduel",
                    "title": "FanDuel",
                    "last_update": "2024-11-09T12:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "San Antonio Spurs", "price": 1.74},
                                {"name": "Utah Jazz", "price": 2.15},
                            ],
                        }
                    ],
                }
            ],
        }

        transformed = connector.transform_odds_data(raw_data)

        assert transformed["game_id"] == "0137c593a0d5d41469e9"
        assert transformed["sport_key"] == "basketball_nba"
        assert transformed["home_team"] == "San Antonio Spurs"
        assert transformed["away_team"] == "Utah Jazz"
        assert "timestamp" in transformed
        assert len(transformed["bookmakers"]) == 1

    def test_publish_to_kafka_success(self, connector, mock_kafka_producer):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        connector.publish_to_kafka("test-topic", test_data)

        connector.producer.send.assert_called_once()
        args, kwargs = connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_publish_to_kafka_failure(self, connector, mock_kafka_producer):
        """Test Kafka publishing failure"""
        connector.producer.send.return_value.get.side_effect = KafkaTimeoutError()

        with pytest.raises(Exception) as exc_info:
            connector.publish_to_kafka("test-topic", {"test": "data"})
        assert "Failed to publish to Kafka" in str(exc_info.value)

    def test_fetch_and_publish_odds(self, connector):
        """Test complete fetch and publish pipeline"""
        mock_response = [
            {
                "id": "test_id",
                "sport_key": "basketball_nba",
                "sport_title": "NBA",
                "commence_time": "2024-11-09T22:00Z",
                "home_team": "Team A",
                "away_team": "Team B",
                "bookmakers": [],
            }
        ]

        session_mock = MagicMock(spec=requests.Session)
        session_mock.get.return_value = MockResponse(mock_response)

        with patch("requests.Session", return_value=session_mock):
            connector.session = session_mock
            connector.fetch_and_publish_odds("basketball_nba")

            session_mock.get.assert_called_once()
            assert connector.producer.send.call_count == 1

    def test_close(self, connector, mock_kafka_producer):
        """Test connector cleanup"""
        connector.close()
        connector.producer.flush.assert_called_once()
        connector.producer.close.assert_called_once()
