from pydantic import BaseModel, ConfigDict
from datetime import datetime


class WeatherData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    weather_id: str
    venue_id: str
    game_id: str
    timestamp: datetime
    temperature: float
    feels_like: float
    humidity: float
    wind_speed: float
    wind_direction: float
    # precipitation_probability: float
    weather_condition: str
    weather_description: str
    visibility: float
    pressure: float
    clouds: float
    location: str
    # uv_index: float
    # details: Optional[dict] = Field(description="Additional weather metrics")
