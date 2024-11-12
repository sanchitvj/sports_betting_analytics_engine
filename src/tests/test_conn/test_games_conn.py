import pytest
from unittest.mock import patch, MagicMock
import requests
from kafka.errors import KafkaTimeoutError

from betflow.api_connectors.games_conn import ESPNConnector

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
                f"{self.status_code} Error",
                response=self
            )


@pytest.fixture
def mock_producer():
    """Mock KafkaProducer"""
    mock = MagicMock()
    future = MagicMock()
    future.get.return_value = None
    mock.send.return_value = future
    return mock


@pytest.fixture
def mock_session():
    """Create mock session with proper configuration"""
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock()
    return session


@pytest.fixture
def connector(mock_producer):
    """Create ESPNConnector with mocked dependencies"""
    with patch('kafka_orch.KafkaProducer', return_value=mock_producer):
        return ESPNConnector(kafka_bootstrap_servers='localhost:9092')
    

class TestESPNConnector:
    def test_init(self, mock_producer):
        """Test connector initialization"""
        with patch('kafka_orch.KafkaProducer', return_value=mock_producer):
            connector = ESPNConnector(kafka_bootstrap_servers='localhost:9092')
            assert connector.base_url == "https://site.api.espn.com/apis/site/v2/sports"
            assert connector.producer is not None

    def test_make_request_success(self, connector):
        """Test successful API request"""
        mock_response = {
            "leagues": [{
                "id": "46",
                "name": "National Basketball Association",
                "abbreviation": "NBA",
                "season": {
                    "year": 2025,
                    "startDate": "2024-09-24T07:00Z",
                    "endDate": "2025-06-28T06:59Z",
                    "type": {"id": "2", "type": 2, "name": "Regular Season"}
                }
            }],
            "season": {"type": 2, "year": 2025},
            "day": {"date": "2024-11-09"},
            "events": []
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch('requests.Session', return_value=session_mock):
            connector.session = session_mock  # Explicitly set the session
            response = connector.make_request('basketball/nba/scoreboard')

            assert response == mock_response
            session_mock.get.assert_called_once_with(
                f"{connector.base_url}/basketball/nba/scoreboard",
                params=None,
                timeout=10
            )

    def test_make_request_rate_limit_error(self, connector):
        """Test rate limit error handling"""
        session_mock = MagicMock(spec=requests.Session)

        def raise_rate_limit(*args, **kwargs):
            raise requests.exceptions.HTTPError(
                "429 Client Error: Too Many Requests",
                response=MagicMock(
                    status_code=429,
                    headers={'Retry-After': '5'}
                )
            )

        session_mock.get.side_effect = raise_rate_limit

        with patch('requests.Session', return_value=session_mock):
            connector.session = session_mock  # Explicitly set the session

            with pytest.raises(Exception) as exc_info:
                connector.make_request('basketball/nba/scoreboard')

            assert "Rate limit exceeded" in str(exc_info.value)
            session_mock.get.assert_called_once_with(
                f"{connector.base_url}/basketball/nba/scoreboard",
                params=None,
                timeout=10
            )

    def test_transform_game_data(self, connector):
        """Test game data transformation"""
        raw_data = {
            'id': '401584668',
            'date': '2024-02-10T00:00Z',
            'status': {'type': {'name': 'SCHEDULED'}},
            'competitions': [{
                'competitors': [
                    {
                        'id': 'home123',
                        'team': {'name': 'Home Team'},
                        'score': '100'
                    },
                    {
                        'id': 'away123',
                        'team': {'name': 'Away Team'},
                        'score': '95'
                    }
                ],
                'venue': {'name': 'Test Arena'}
            }]
        }

        transformed = connector.transform_game_data(raw_data)

        assert transformed['game_id'] == '401584668'
        assert transformed['start_time'] == '2024-02-10T00:00Z'
        assert transformed['status'] == 'SCHEDULED'
        assert transformed['home_team']['name'] == 'Home Team'
        assert transformed['away_team']['name'] == 'Away Team'
        assert transformed['venue'] == 'Test Arena'
        assert 'timestamp' in transformed

    def test_publish_to_kafka_success(self, connector, mock_producer):
        """Test successful Kafka message publishing"""
        test_data = {'test': 'data'}

        with patch.object(connector, 'producer', mock_producer):
            connector.publish_to_kafka('test-topic', test_data)

            connector.producer.send.assert_called_once()
            args, kwargs = connector.producer.send.call_args
            assert args[0] == 'test-topic'
            assert kwargs['value'] == test_data

    def test_publish_to_kafka_failure(self, connector, mock_producer):
        """Test Kafka publishing failure"""
        with patch.object(connector, 'producer', mock_producer):
            mock_producer.send.return_value.get.side_effect = KafkaTimeoutError()

            with pytest.raises(Exception) as exc_info:
                connector.publish_to_kafka('test-topic', {'test': 'data'})
            assert "Error publishing to Kafka" in str(exc_info.value)

    def test_close(self, connector, mock_producer):
        """Test connector cleanup"""
        with patch.object(connector, 'producer', mock_producer):
            connector.close()
            mock_producer.flush.assert_called_once()
            mock_producer.close.assert_called_once()