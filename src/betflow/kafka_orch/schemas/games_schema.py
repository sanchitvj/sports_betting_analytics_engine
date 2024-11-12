from typing import Dict, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

class BaseGameStats(BaseModel):
    game_id: str
    sport_type: str = Field(..., description="NBA, NFL, or MLB")
    start_time: datetime
    venue_id: str
    status: str
    home_team_id: str
    away_team_id: str
    season: int
    season_type: str
    broadcast: Optional[List[str]]

class NFLGameStats(BaseGameStats):
    current_quarter: Optional[int]
    time_remaining: Optional[str]
    down: Optional[int]
    yards_to_go: Optional[int]
    possession: Optional[str]
    score: Dict[str, int]
    stats: Dict[str, Dict[str, float]] = Field(
        description="Team statistics including passing, rushing, etc."
    )

class NBAGameStats(BaseGameStats):
    current_period: Optional[int]
    time_remaining: Optional[str]
    score: Dict[str, int]
    stats: Dict[str, Dict[str, float]] = Field(
        description="Team statistics including rebounds, assists, etc."
    )

class MLBGameStats(BaseGameStats):
    current_inning: Optional[int]
    inning_half: Optional[str]
    outs: Optional[int]
    bases_occupied: List[int]
    score: Dict[str, int]
    stats: Dict[str, Dict[str, float]] = Field(
        description="Team statistics including hits, errors, etc."
    )