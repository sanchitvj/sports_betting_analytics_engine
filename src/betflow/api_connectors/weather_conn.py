import json
import time
from typing import Dict, Any, Optional
import requests
from datetime import datetime
from kafka import KafkaProducer
from betflow.api_connectors.conn_utils import RateLimiter
from betflow.kafka_orch.schemas import WeatherData


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class OpenWeatherConnector:
    """Connector for OpenWeather API with Kafka integration."""

    def __init__(
        self,
        api_key: str,
        kafka_bootstrap_servers: str,
        base_url: str = "https://api.openweathermap.org/data/2.5",
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
            value_serializer=lambda v: json.dumps(v, cls=DateTimeEncoder).encode(
                "utf-8"
            ),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make a rate-limited request to OpenWeather API."""
        self.rate_limiter.wait_if_needed()

        if params is None:
            params = {}
        # params["appid"] = self.api_key

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

    def transform_weather_data(
        self, raw_data: Dict[str, Any], venue_id: str, game_id: str
    ) -> Dict[str, Any]:
        """Transform raw weather data to standardized format."""
        try:
            # Convert timestamp to ISO format string
            timestamp = datetime.fromtimestamp(raw_data["dt"])

            weather_data = {
                "weather_id": f"weather_{venue_id}_{int(time.time())}",
                "venue_id": venue_id,
                "game_id": game_id,
                "timestamp": timestamp.isoformat(),  # Convert to ISO format string
                "temperature": raw_data["main"]["temp"] - 273.15,  # Convert K to C
                "feels_like": raw_data["main"]["feels_like"] - 273.15,  # Convert K to C
                "humidity": raw_data["main"]["humidity"],
                "wind_speed": raw_data["wind"]["speed"],
                "wind_direction": float(raw_data["wind"]["deg"]),
                "weather_condition": raw_data["weather"][0]["main"],
                "weather_description": raw_data["weather"][0]["description"],
                "visibility": raw_data["visibility"] / 1000,  # Convert to km
                "pressure": raw_data["main"]["pressure"],
                # "precipitation_probability": 0.0,
                # "uv_index": 0.0,
                "clouds": raw_data.get("clouds", {}).get("all", 0),
                # "details": {
                #     "clouds": raw_data.get("clouds", {}).get("all", 0),
                #     "rain_1h": raw_data.get("rain", {}).get("1h", 0),
                #     "snow_1h": raw_data.get("snow", {}).get("1h", 0),
                # },
                "location": raw_data["name"],
                #     "lat": str(raw_data["coord"]["lat"]),  # Convert to string
                #     "lon": str(raw_data["coord"]["lon"]),  # Convert to string
                #     "city": raw_data["name"],
                #     "country": raw_data["sys"]["country"],
                # },
            }

            # Validate against schema
            return WeatherData(**weather_data).model_dump()

        except Exception as e:
            raise ValueError(f"Failed to transform weather data: {e}")

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    async def fetch_and_publish_weather(
        self,
        topic_name: str,
        city: str,
        venue_id: str,
        game_id: str = None,
        lat: float = None,
        lon: float = None,
    ) -> Dict[str, Any]:
        """Fetch weather data for a city and publish to Kafka."""
        try:
            # Make API request with proper parameters
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,  # Add API key here
                # "units": "standard"  # Use Kelvin for consistency
            }

            # Add either city or coordinates
            if lat is not None and lon is not None:
                params.update({"lat": lat, "lon": lon})
            else:
                params["q"] = city

            # Make the request
            raw_data = self.make_request("weather", params=params)

            # Transform data
            transformed_data = self.transform_weather_data(
                raw_data, venue_id=venue_id, game_id=game_id
            )

            # Publish to Kafka
            self.publish_to_kafka(
                topic=topic_name,
                data=transformed_data,
            )

            return transformed_data

        except Exception as e:
            raise Exception(f"Failed to fetch/publish weather for {city}: {e}")

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
        base_url: str = "https://api.open-meteo.com/v1",
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
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            retries=3,
            acks="all",
        )

    def make_request(
        self, endpoint: str, params: Optional[Dict] = None
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
        hourly = raw_data.get("hourly", {})
        return {
            "location": {
                "latitude": raw_data.get("latitude"),
                "longitude": raw_data.get("longitude"),
                "timezone": raw_data.get("timezone"),
                "elevation": raw_data.get("elevation"),
            },
            "forecast": [
                {
                    "time": time,
                    "temperature": temp,
                    "precipitation": precip,
                    "wind_speed": wind,
                }
                for time, temp, precip, wind in zip(
                    hourly.get("time", []),
                    hourly.get("temperature_2m", []),
                    hourly.get("precipitation", []),
                    hourly.get("windspeed_10m", []),
                )
            ],
            "timestamp": int(time.time()),
        }

    def publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> None:
        """Publish data to Kafka topic."""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
        except Exception as e:
            raise Exception(f"Failed to publish to Kafka: {e}")

    def fetch_and_publish_weather(
        self, latitude: float, longitude: float, days: int = 7
    ) -> None:
        """Fetch forecast data and publish to Kafka."""
        try:
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "hourly": "temperature_2m,precipitation,windspeed_10m",
                "forecast_days": days,
            }

            raw_data = self.make_request("forecast", params=params)
            transformed_data = self.transform_forecast_data(raw_data)
            self.publish_to_kafka(
                f"weather.forecast.{latitude}_{longitude}", transformed_data
            )

        except Exception as e:
            raise Exception(f"Failed to fetch and publish forecast: {e}")

    def close(self) -> None:
        """Clean up resources."""
        self.producer.flush()
        self.producer.close()
        self.session.close()


# def main():
#     """Main function to demonstrate usage."""
#     openweather_key = os.getenv("OPENWEATHER_API_KEY")
#
#     # Initialize connectors
#     openweather = OpenWeatherConnector(
#         api_key=openweather_key, kafka_bootstrap_servers="localhost:9092"
#     )
#
#     openmeteo = OpenMeteoConnector(kafka_bootstrap_servers="localhost:9092")
#
#     try:
#         # Fetch current weather
#         openweather.fetch_and_publish_weather("London")
#
#         # Fetch forecast
#         openmeteo.fetch_and_publish_forecast(latitude=51.5074, longitude=-0.1278)
#
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         openweather.close()
#         openmeteo.close()
#
#
# if __name__ == "__main__":
#     main()
