from typing import Dict, Any
from datetime import datetime
from betflow.kafka_orch.schemas import WeatherData
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, lit, current_timestamp
from pyspark.sql.types import StringType


class WeatherTransformer:
    """Transform weather data from different sources."""

    def transform_openweather(self, df: DataFrame) -> DataFrame:
        """Transform streaming DataFrame.

        Args:
            df: Input DataFrame with parsed weather data

        Returns:
            DataFrame: Transformed weather data
        """

        # Create UDF for wind direction
        @udf(returnType=StringType())
        def get_wind_direction_udf(degrees):
            if degrees is None:
                return "None"  # Default to North for None values
            try:
                return self._get_wind_direction(float(degrees))
            except (ValueError, TypeError):
                return "None"  # Default to North for invalid values

        # Apply transformations
        try:
            return df.select(
                # Base fields
                lit("weather_id"),
                lit("venue_id"),
                lit("game_id"),
                col("timestamp"),
                # Temperature metrics
                col("temperature"),
                col("feels_like"),
                col("humidity"),
                col("pressure"),
                # Wind metrics
                col("wind_speed"),
                get_wind_direction_udf(col("wind_direction")).alias("wind_direction"),
                # Additional metrics
                col("visibility"),  # Convert to km
                col("clouds"),
                # Weather conditions
                col("weather_condition"),
                col("weather_description"),
                # Location data
                col("location"),
            ).withColumn("processing_time", current_timestamp())
        except Exception as e:
            raise ValueError(f"Failed to transform OpenWeather data: {e}")

    def transform_openmeteo(
        self, raw_data: Dict[str, Any], venue_id: str, game_id: str
    ) -> Dict[str, Any]:
        """Transform Open-Meteo API data."""
        weather_data = {
            "weather_id": f"weather_{venue_id}_{int(datetime.now().timestamp())}",
            "venue_id": venue_id,
            "game_id": game_id,
            "timestamp": datetime.fromtimestamp(raw_data["time"]),
            "temperature": raw_data["temperature_2m"],
            "feels_like": raw_data["apparent_temperature"],
            "humidity": raw_data["relative_humidity_2m"],
            "wind_speed": raw_data["wind_speed_10m"],
            "wind_direction": self._get_wind_direction(raw_data["wind_direction_10m"]),
            "precipitation_probability": raw_data["precipitation_probability"],
            "weather_condition": self._get_weather_condition(raw_data),
            "visibility": raw_data.get("visibility", 10),
            "pressure": raw_data["surface_pressure"],
            "uv_index": raw_data.get("uv_index", 0),
            "details": {
                "clouds": raw_data.get("cloud_cover"),
                "rain_1h": raw_data.get("rain", 0),  # Default to 0
                "snow_1h": raw_data.get("snowfall", 0),  # Default to 0
            },
        }

        return WeatherData(**weather_data).model_dump()

    @staticmethod
    def _get_wind_direction(degrees: float) -> str:
        """Convert wind degrees to cardinal direction.

        Args:
            degrees: Wind direction in degrees (0-360, where 0/360 is North)

        Returns:
            str: Cardinal direction (N, NE, E, SE, S, SW, W, NW)

        Note:
            Direction ranges:
            N   : 348.75 - 11.25
            NE  : 11.25  - 78.75
            E   : 78.75  - 101.25
            SE  : 101.25 - 168.75
            S   : 168.75 - 191.25
            SW  : 191.25 - 258.75
            W   : 258.75 - 281.25
            NW  : 281.25 - 348.75
        """
        # Normalize degrees to be between 0 and 360
        degrees = degrees % 360

        # Define direction boundaries (each direction spans 45 degrees)
        boundaries = [
            (348.75, 11.25, "N"),  # North spans across 0
            (11.25, 78.75, "NE"),
            (78.75, 101.25, "E"),
            (101.25, 168.75, "SE"),
            (168.75, 191.25, "S"),
            (191.25, 258.75, "SW"),
            (258.75, 281.25, "W"),
            (281.25, 348.75, "NW"),
        ]

        # Special case for North when degrees is between 348.75 and 360
        # or between 0 and 11.25
        if degrees >= 348.75 or degrees < 11.25:
            return "N"

        # Find matching range
        for start, end, direction in boundaries:
            if start <= degrees < end:
                return direction

        # Default to North (should never reach here due to normalization)
        return "N"

    @staticmethod
    def _get_weather_condition(data: Dict[str, Any]) -> str:
        """Determine weather condition from Open-Meteo data."""
        if data.get("snowfall", 0) > 0:
            return "Snow"
        elif data.get("rain", 0) > 0:
            return "Rain"
        elif data.get("cloud_cover", 0) > 80:
            return "Cloudy"
        elif data.get("cloud_cover", 0) > 20:
            return "Partly Cloudy"
        return "Clear"
