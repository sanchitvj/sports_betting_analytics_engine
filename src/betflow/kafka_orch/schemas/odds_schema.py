from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Dict, Optional, Any


class OddsData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)  # Add this line

    odds_id: str
    game_id: str
    sport_type: str
    bookmaker_id: str
    timestamp: datetime
    market_type: str = Field(..., description="moneyline, spread, totals, etc.")
    odds_value: float
    spread_value: Optional[float] = None
    total_value: Optional[float] = None
    probability: Optional[float] = None
    volume: Optional[Dict[str, float]] = None
    movement: Dict[str, float] = Field(description="Odds movement from opening")
    status: str
    metadata: Optional[Dict[str, Any]] = None
