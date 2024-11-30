import pytest
from datetime import datetime
from betflow.spark_streaming.event_transformer import WeatherTransformer


class TestWeatherTransformer:
    @pytest.fixture
    def weather_transformer(self):
        """Create WeatherTransformer instance."""
        return WeatherTransformer()

    @pytest.fixture
    def openweather_data(self):
        """Fixture for OpenWeather API data."""
        return {
            "dt": int(datetime.now().timestamp()),
            "main": {
                "temp": 20.5,
                "feels_like": 21.0,
                "humidity": 65,
                "pressure": 1015,
            },
            "wind": {"speed": 5.2, "deg": 45},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "visibility": 10000,
            "pop": 0.2,
            "uvi": 4.5,
            "clouds": {"all": 20},
            "rain": {"1h": 0.5},
        }

    @pytest.fixture
    def openmeteo_data(self):
        """Fixture for Open-Meteo API data."""
        return {
            "time": int(datetime.now().timestamp()),
            "temperature_2m": 20.5,
            "apparent_temperature": 21.0,
            "relative_humidity_2m": 65,
            "surface_pressure": 1015,
            "wind_speed_10m": 5.2,
            "wind_direction_10m": 45,
            "precipitation_probability": 20,
            "cloud_cover": 20,
            "visibility": 10,
            "uv_index": 4.5,
            "rain": 0.5,
            "snowfall": 0,
        }

    def test_transform_openweather_valid_data(
        self, weather_transformer, openweather_data
    ):
        """Test OpenWeather data transformation with valid data."""
        venue_id = "venue123"
        game_id = "game123"

        result = weather_transformer.transform_openweather(
            openweather_data, venue_id, game_id
        )

        # Verify basic fields
        assert result["venue_id"] == venue_id
        assert result["game_id"] == game_id
        assert isinstance(result["weather_id"], str)
        assert isinstance(result["timestamp"], datetime)

        # Verify weather metrics
        assert result["temperature"] == openweather_data["main"]["temp"]
        assert result["feels_like"] == openweather_data["main"]["feels_like"]
        assert result["humidity"] == openweather_data["main"]["humidity"]
        assert result["wind_speed"] == openweather_data["wind"]["speed"]
        assert result["wind_direction"] == "NE"  # 45 degrees maps to NE
        assert result["precipitation_probability"] == openweather_data["pop"] * 100
        assert result["weather_condition"] == openweather_data["weather"][0]["main"]
        assert result["visibility"] == openweather_data["visibility"] / 10000
        assert result["pressure"] == openweather_data["main"]["pressure"]
        assert result["uv_index"] == openweather_data["uvi"]

    def test_transform_openweather_missing_optional(
        self, weather_transformer, openweather_data
    ):
        """Test OpenWeather transformation with missing optional fields."""
        # Remove optional fields
        del openweather_data["uvi"]
        del openweather_data["rain"]
        del openweather_data["clouds"]

        result = weather_transformer.transform_openweather(
            openweather_data, "venue123", "game123"
        )

        assert result["uv_index"] == 0
        assert result["details"]["rain_1h"] == 0
        assert result["details"]["clouds"] is None

    def test_transform_openmeteo_valid_data(self, weather_transformer, openmeteo_data):
        """Test Open-Meteo data transformation with valid data."""
        venue_id = "venue123"
        game_id = "game123"

        result = weather_transformer.transform_openmeteo(
            openmeteo_data, venue_id, game_id
        )

        # Verify basic fields
        assert result["venue_id"] == venue_id
        assert result["game_id"] == game_id
        assert isinstance(result["weather_id"], str)
        assert isinstance(result["timestamp"], datetime)

        # Verify weather metrics
        assert result["temperature"] == openmeteo_data["temperature_2m"]
        assert result["feels_like"] == openmeteo_data["apparent_temperature"]
        assert result["humidity"] == openmeteo_data["relative_humidity_2m"]
        assert result["wind_speed"] == openmeteo_data["wind_speed_10m"]
        assert result["wind_direction"] == "NE"  # 45 degrees maps to NE
        assert (
            result["precipitation_probability"]
            == openmeteo_data["precipitation_probability"]
        )
        assert result["pressure"] == openmeteo_data["surface_pressure"]
        assert result["uv_index"] == openmeteo_data["uv_index"]

    def test_transform_openmeteo_missing_optional(
        self, weather_transformer, openmeteo_data
    ):
        """Test Open-Meteo transformation with missing optional fields."""
        # Remove optional fields
        del openmeteo_data["uv_index"]
        del openmeteo_data["rain"]
        del openmeteo_data["visibility"]

        result = weather_transformer.transform_openmeteo(
            openmeteo_data, "venue123", "game123"
        )

        assert result["uv_index"] == 0
        assert result["details"]["rain_1h"] == 0
        assert result["visibility"] == 10  # Default value

    def test_wind_direction_conversion(self, weather_transformer):
        """Test wind direction degree to cardinal direction conversion."""
        test_cases = [
            (0, "N"),
            (45, "NE"),
            (90, "E"),
            (135, "SE"),
            (180, "S"),
            (225, "SW"),
            (270, "W"),
            (315, "NW"),
            (360, "N"),
        ]

        for degrees, expected in test_cases:
            assert weather_transformer._get_wind_direction(degrees) == expected

    def test_weather_condition_determination(self, weather_transformer):
        """Test weather condition determination from Open-Meteo data."""
        # Test snow condition
        assert weather_transformer._get_weather_condition({"snowfall": 1.0}) == "Snow"

        # Test rain condition
        assert (
            weather_transformer._get_weather_condition({"rain": 1.0, "snowfall": 0})
            == "Rain"
        )

        # Test cloudy conditions
        assert (
            weather_transformer._get_weather_condition({"cloud_cover": 90}) == "Cloudy"
        )
        assert (
            weather_transformer._get_weather_condition({"cloud_cover": 50})
            == "Partly Cloudy"
        )

        # Test clear condition
        assert (
            weather_transformer._get_weather_condition({"cloud_cover": 10}) == "Clear"
        )

    def test_invalid_wind_direction(self, weather_transformer):
        """Test handling of invalid wind direction values."""
        # Test degree normalization
        assert weather_transformer._get_wind_direction(-45) == "NW"  # -45 -> 315 -> NW
        assert weather_transformer._get_wind_direction(405) == "NE"  # 405 -> 45 -> NE

        # Test edge cases
        assert weather_transformer._get_wind_direction(0) == "N"
        assert weather_transformer._get_wind_direction(360) == "N"
        assert weather_transformer._get_wind_direction(-360) == "N"

        # Test range boundaries
        assert weather_transformer._get_wind_direction(22.5) == "NE"
        assert weather_transformer._get_wind_direction(67.5) == "NE"
        assert weather_transformer._get_wind_direction(337.5) == "NW"

    def test_details_field_handling(self, weather_transformer, openweather_data):
        """Test handling of details field."""
        result = weather_transformer.transform_openweather(
            openweather_data, "venue123", "game123"
        )

        assert "details" in result
        assert isinstance(result["details"], dict)
        assert "clouds" in result["details"]
        assert "rain_1h" in result["details"]
        assert "snow_1h" in result["details"]
