# test_espn_connector.py
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from kafka import KafkaProducer
import requests

from betflow.api_connectors.games_conn import ESPNConnector
from betflow.api_connectors.conn_utils import RateLimiter


@pytest.fixture
def mock_kafka_producer():
    """Fixture for mocked KafkaProducer."""
    with patch('kafka.KafkaProducer') as mock:
        producer = Mock()
        producer.send.return_value.get.return_value = None
        mock.return_value = producer
        yield mock


@pytest.fixture
def mock_session():
    """Fixture for mocked requests.Session."""
    with patch('requests.Session') as mock:
        yield mock


@pytest.fixture
def connector(mock_kafka_producer, mock_session):
    """Fixture for ESPNConnector instance with mocked dependencies."""
    return ESPNConnector(kafka_bootstrap_servers='localhost:9092')


class TestESPNConnector:
    """Test suite for ESPNConnector class."""

    def test_init(self, connector):
        """Test connector initialization."""
        assert connector.base_url == "https://site.api.espn.com/apis/site/v2/sports"
        assert isinstance(connector.rate_limiter, RateLimiter)

    def test_make_request_success(self, connector, mock_session):
        """Test successful API request."""
        expected_response = {'data': 'test'}
        mock_response = Mock()
        mock_response.json.return_value = expected_response
        mock_session.return_value.get.return_value = mock_response

        response = connector.make_request('test/endpoint')
        assert response == expected_response
        mock_session.return_value.get.assert_called_once_with(
            f"{connector.base_url}/test/endpoint",
            params=None,
            timeout=10
        )

    def test_make_request_rate_limit_error(self, connector, mock_session):
        """Test rate limit error handling."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {'Retry-After': '5'}
        mock_session.return_value.get.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )

        with pytest.raises(Exception) as exc_info:
            connector.make_request('test/endpoint')
        assert "Rate limit exceeded" in str(exc_info.value)

    @pytest.mark.parametrize("raw_data", [
        {
            'id': '12345',
            'date': '2024-01-01T20:00Z',
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
    ])
    def test_transform_game_data(self, connector, raw_data):
        """Test game data transformation."""
        transformed = connector.transform_game_data(raw_data)

        assert transformed['game_id'] == '12345'
        assert transformed['start_time'] == '2024-01-01T20:00Z'
        assert transformed['status'] == 'SCHEDULED'
        assert transformed['home_team']['name'] == 'Home Team'
        assert transformed['away_team']['name'] == 'Away Team'
        assert transformed['venue'] == 'Test Arena'
        assert 'timestamp' in transformed

    def test_publish_to_kafka_success(self, connector, mock_kafka_producer):
        """Test successful Kafka message publishing."""
        test_data = {'test': 'data'}
        connector.publish_to_kafka('test-topic', test_data)

        connector.producer.send.assert_called_once()
        connector.producer.send.assert_called_with('test-topic', value=test_data)

    def test_publish_to_kafka_failure(self, connector, mock_kafka_producer):
        """Test Kafka publishing failure."""
        connector.producer.send.return_value.get.side_effect = Exception('Kafka error')

        with pytest.raises(Exception) as exc_info:
            connector.publish_to_kafka('test-topic', {'test': 'data'})
        assert "Error publishing to Kafka" in str(exc_info.value)

    @patch('espn_connector.ESPNConnector.make_request')
    @patch('espn_connector.ESPNConnector.transform_game_data')
    @patch('espn_connector.ESPNConnector.publish_to_kafka')
    def test_fetch_and_publish_games(
        self, mock_publish, mock_transform, mock_request, connector
    ):
        """Test complete fetch and publish pipeline."""
        # Mock API response
        mock_request.return_value = {
            'events': [{'id': '1'}, {'id': '2'}]
        }

        # Mock transformation
        mock_transform.side_effect = lambda x: {'transformed': x['id']}

        connector.fetch_and_publish_games('basketball', 'nba')

        # Verify API call
        mock_request.assert_called_once_with('basketball/nba/scoreboard')

        # Verify transformations
        assert mock_transform.call_count == 2

        # Verify Kafka publishing
        assert mock_publish.call_count == 2
        mock_publish.assert_any_call(
            'espn.basketball.nba.games',
            {'transformed': '1'}
        )

    def test_close(self, connector):
        """Test connector cleanup."""
        connector.close()
        connector.producer.flush.assert_called_once()
        connector.producer.close.assert_called_once()
