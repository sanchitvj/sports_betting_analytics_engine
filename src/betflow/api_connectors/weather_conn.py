# weather_connectors.py
import os
import json
import time
from typing import Dict, Any, Optional
import requests
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter


class OpenWeatherConnector:
    """Connector for OpenWeather API with Kafka integration."""

    def __init__(
        self,
        api_key: str,
        kafka_bootstrap_servers: str,
        base_url: str = "https://api.openweathermap.org/data/2.5"
    ) -> None:
        """Initialize OpenWeather connector.

        Args:
            api_key: OpenWeather API key
            kafka_bootstrap_servers: Kafka bootstrap servers
            base_url: Base URL for OpenWeather API
        """
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        # Free tier: 60 calls/minute
        self.rate_limiter = RateLimiter(requests_per_second=1)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',
            retries=3,
            acks='all'
        )

    def make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to OpenWeather API."""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}
        params['appid'] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise Exception("Rate limit exceeded")
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def transform_weather_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw weather data to our schema."""
        return {
            'location': {
                'name': raw_data.get('name'),
                'country': raw_data.get('sys', {}).get('country'),
                'coordinates': {
                    'lat': raw_data.get('coord', {}).get('lat'),
                    'lon': raw_data.get('coord', {}).get('lon')
                }
            },
            'weather': {
                'condition': raw_data.get('weather', [{}])[0].get('main'),
                'description': raw_data.get('weather', [{}])[0].get('description'),
                'temperature': raw_data.get('main', {}).get('temp'),
                'feels_like': raw_data.get('main', {}).get('feels_like'),
                'humidity': raw_data.get('main', {}).get('humidity'),
                'pressure': raw_data.get('main', {}).get('pressure'),
                'wind_speed': raw_data.get('wind', {}).get('speed'),
                'wind_direction': raw_data.get('wind', {}).get('deg')
            },
            'timestamp': int(time.time())
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_weather(self, city: str) -> None:
        """Fetch weather data for a city and publish to Kafka."""
        try:
            params = {
                'q': city,
                'units': 'metric'
            }
            raw_data = self.make_request('weather', params=params)
            transformed_data = self.transform_weather_data(raw_data)
            self.publish_to_kafka(f"weather.current.{city}", transformed_data)

        except Exception as e:
            raise Exception(f"Failed to fetch and publish weather: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


class OpenMeteoConnector:
    """Connector for Open-Meteo API with Kafka integration."""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        base_url: str = "https://api.open-meteo.com/v1"
    ) -> None:
        """Initialize Open-Meteo connector.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            base_url: Base URL for Open-Meteo API
        """
        self.base_url = base_url
        self.session = requests.Session()
        # Free tier: No strict limit, but use reasonable rate
        self.rate_limiter = RateLimiter(requests_per_second=2)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',
            retries=3,
            acks='all'
        )

    def make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to Open-Meteo API."""
        self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            raise Exception(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {e}")

    def transform_forecast_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw forecast data to our schema."""
        hourly = raw_data.get('hourly', {})
        return {
            'location': {
                'latitude': raw_data.get('latitude'),
                'longitude': raw_data.get('longitude'),
                'timezone': raw_data.get('timezone'),
                'elevation': raw_data.get('elevation')
            },
            'forecast': [
                {
                    'time': time,
                    'temperature': temp,
                    'precipitation': precip,
                    'wind_speed': wind
                }
                for time, temp, precip, wind in zip(
                    hourly.get('time', []),
                    hourly.get('temperature_2m', []),
                    hourly.get('precipitation', []),
                    hourly.get('windspeed_10m', [])
                )
            ],
            'timestamp': int(time.time())
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_forecast(
        self,
        latitude: float,
        longitude: float,
        days: int = 7
    ) -> None:
        """Fetch forecast data and publish to Kafka."""
        try:
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'hourly': 'temperature_2m,precipitation,windspeed_10m',
                'forecast_days': days
            }

            raw_data = self.make_request('forecast', params=params)
            transformed_data = self.transform_forecast_data(raw_data)
            self.publish_to_kafka(
                f"weather.forecast.{latitude}_{longitude}",
                transformed_data
            )

        except Exception as e:
            raise Exception(f"Failed to fetch and publish forecast: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


def main():
    """Main function to demonstrate usage."""
    openweather_key = os.getenv('OPENWEATHER_API_KEY')

    # Initialize connectors
    openweather = OpenWeatherConnector(
        api_key=openweather_key,
        kafka_bootstrap_servers='localhost:9092'
    )

    openmeteo = OpenMeteoConnector(
        kafka_bootstrap_servers='localhost:9092'
    )

    try:
        # Fetch current weather
        openweather.fetch_and_publish_weather('London')

        # Fetch forecast
        openmeteo.fetch_and_publish_forecast(
            latitude=51.5074,
            longitude=-0.1278
        )

    except Exception as e:
        print(f"Error: {e}")
    finally:
        openweather.close()
        openmeteo.close()


if __name__ == "__main__":
    main()