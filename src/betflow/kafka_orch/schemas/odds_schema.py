from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Optional

class OddsData(BaseModel):
    odds_id: str
    game_id: str
    sport_type: str
    bookmaker_id: str
    timestamp: datetime
    market_type: str = Field(..., description="moneyline, spread, totals, etc.")
    odds_value: float
    spread_value: Optional[float]
    total_value: Optional[float]
    probability: Optional[float]
    volume: Optional[Dict[str, float]]
    movement: Dict[str, float] = Field(
        description="Odds movement from opening"
    )
    status: str
    metadata: Optional[Dict[str, any]]