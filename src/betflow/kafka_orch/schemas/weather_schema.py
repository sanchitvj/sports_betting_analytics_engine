from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class WeatherData(BaseModel):
    weather_id: str
    venue_id: str
    game_id: str
    timestamp: datetime
    temperature: float
    feels_like: float
    humidity: float
    wind_speed: float
    wind_direction: str
    precipitation_probability: float
    weather_condition: str
    visibility: float
    pressure: float
    uv_index: float
    details: Optional[dict] = Field(
        description="Additional weather metrics"
    )