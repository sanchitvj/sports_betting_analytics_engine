# test_weather_connectors.py
import pytest
from unittest.mock import patch, MagicMock
import requests
from betflow.api_connectors.weather_conn import OpenWeatherConnector, OpenMeteoConnector


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


@pytest.fixture(autouse=True)
def mock_kafka():
    """Mock KafkaProducer at module level"""
    with patch('betflow.api_connectors.weather_conn.KafkaProducer') as mock:
        mock_instance = MagicMock()
        mock_instance.send.return_value.get.return_value = None
        mock.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_session():
    """Create mock session"""
    session = MagicMock(spec=requests.Session)
    session.get = MagicMock()
    return session


class TestOpenWeatherConnector:
    @pytest.fixture
    def openweather_connector(self):
        """Create OpenWeatherConnector instance"""
        return OpenWeatherConnector(
            api_key="test_api_key",
            kafka_bootstrap_servers='localhost:9092'
        )

    def test_init(self, openweather_connector):
        """Test connector initialization"""
        assert openweather_connector.api_key == "test_api_key"
        assert openweather_connector.base_url == "https://api.openweathermap.org/data/2.5"
        assert openweather_connector.producer is not None

    def test_make_request_success(self, openweather_connector):
        """Test successful API request"""
        mock_response = {
            "name": "London",
            "sys": {"country": "GB"},
            "coord": {"lat": 51.51, "lon": -0.13},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "main": {
                "temp": 15.5,
                "feels_like": 14.8,
                "humidity": 76,
                "pressure": 1015
            },
            "wind": {"speed": 4.1, "deg": 280}
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch('requests.Session', return_value=session_mock):
            openweather_connector.session = session_mock
            response = openweather_connector.make_request('weather', {'q': 'London'})

            assert response == mock_response
            session_mock.get.assert_called_once()

    def test_transform_weather_data(self, openweather_connector):
        """Test weather data transformation"""
        raw_data = {
            "name": "London",
            "sys": {"country": "GB"},
            "coord": {"lat": 51.51, "lon": -0.13},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "main": {
                "temp": 15.5,
                "feels_like": 14.8,
                "humidity": 76,
                "pressure": 1015
            },
            "wind": {"speed": 4.1, "deg": 280}
        }

        transformed = openweather_connector.transform_weather_data(raw_data)

        assert transformed["location"]["name"] == "London"
        assert transformed["location"]["country"] == "GB"
        assert transformed["weather"]["condition"] == "Clear"
        assert transformed["weather"]["temperature"] == 15.5
        assert "timestamp" in transformed

    def test_publish_to_kafka_success(self, openweather_connector, mock_kafka):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        openweather_connector.publish_to_kafka("test-topic", test_data)

        openweather_connector.producer.send.assert_called_once()
        args, kwargs = openweather_connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_close(self, openweather_connector, mock_kafka):
        """Test connector cleanup"""
        openweather_connector.close()
        openweather_connector.producer.flush.assert_called_once()
        openweather_connector.producer.close.assert_called_once()


class TestOpenMeteoConnector:
    @pytest.fixture
    def openmeteo_connector(self):
        """Create OpenMeteoConnector instance"""
        return OpenMeteoConnector(
            kafka_bootstrap_servers='localhost:9092'
        )

    def test_init(self, openmeteo_connector):
        """Test connector initialization"""
        assert openmeteo_connector.base_url == "https://api.open-meteo.com/v1"
        assert openmeteo_connector.producer is not None

    def test_make_request_success(self, openmeteo_connector):
        """Test successful API request"""
        mock_response = {
            "latitude": 51.51,
            "longitude": -0.13,
            "timezone": "Europe/London",
            "elevation": 25,
            "hourly": {
                "time": ["2024-02-09T00:00", "2024-02-09T01:00"],
                "temperature_2m": [15.5, 15.2],
                "precipitation": [0, 0.2],
                "windspeed_10m": [4.1, 4.3]
            }
        }

        session_mock = MagicMock(spec=requests.Session)
        response_mock = MockResponse(mock_response)
        session_mock.get.return_value = response_mock

        with patch('requests.Session', return_value=session_mock):
            openmeteo_connector.session = session_mock
            response = openmeteo_connector.make_request('forecast', {
                'latitude': 51.51,
                'longitude': -0.13
            })

            assert response == mock_response
            session_mock.get.assert_called_once()

    def test_transform_forecast_data(self, openmeteo_connector):
        """Test forecast data transformation"""
        raw_data = {
            "latitude": 51.51,
            "longitude": -0.13,
            "timezone": "Europe/London",
            "elevation": 25,
            "hourly": {
                "time": ["2024-02-09T00:00", "2024-02-09T01:00"],
                "temperature_2m": [15.5, 15.2],
                "precipitation": [0, 0.2],
                "windspeed_10m": [4.1, 4.3]
            }
        }

        transformed = openmeteo_connector.transform_forecast_data(raw_data)

        assert transformed["location"]["latitude"] == 51.51
        assert transformed["location"]["longitude"] == -0.13
        assert len(transformed["forecast"]) == 2
        assert "timestamp" in transformed

    def test_publish_to_kafka_success(self, openmeteo_connector, mock_kafka):
        """Test successful Kafka message publishing"""
        test_data = {"test": "data"}
        openmeteo_connector.publish_to_kafka("test-topic", test_data)

        openmeteo_connector.producer.send.assert_called_once()
        args, kwargs = openmeteo_connector.producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == test_data

    def test_close(self, openmeteo_connector, mock_kafka):
        """Test connector cleanup"""
        openmeteo_connector.close()
        openmeteo_connector.producer.flush.assert_called_once()
        openmeteo_connector.producer.close.assert_called_once()