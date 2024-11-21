from typing import Dict, Optional, List
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class BaseGameStats(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
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
    model_config = ConfigDict(arbitrary_types_allowed=True)
    game_id: str
    start_time: str
    status_state: str
    status_detail: str
    status_description: str
    period: int
    clock: str
    home_team_name: str
    home_team_abbreviation: str
    home_team_score: str
    home_team_record: str
    home_passing_display_value: str
    home_passing_value: float
    home_passing_athlete_name: str
    home_passing_athlete_position: str
    home_rushing_display_value: str
    home_rushing_value: float
    home_rushing_athlete_name: str
    home_rushing_athlete_position: str
    home_receive_display_value: str
    home_receive_value: float
    home_receive_athlete_name: str
    home_receive_athlete_position: str
    home_team_linescores: List[float]
    away_team_name: str
    away_team_abbreviation: str
    away_team_score: str
    away_team_record: str
    away_passing_display_value: str
    away_passing_value: float
    away_passing_athlete_name: str
    away_passing_athlete_position: str
    away_rushing_display_value: str
    away_rushing_value: str
    away_rushing_athlete_name: str
    away_rushing_athlete_position: str
    away_receive_display_value: str
    away_receive_value: float
    away_receive_athlete_name: str
    away_receive_athlete_position: str
    away_team_linescores: List[float]
    venue_name: str
    venue_city: str
    venue_state: str
    venue_indoor: bool
    broadcasts: List[str]
    timestamp: datetime


class CFBGameStats(BaseGameStats):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    game_id: str
    start_time: str
    status_state: str
    status_detail: str
    status_description: str
    period: int
    clock: str
    home_team_name: str
    home_team_abbreviation: str
    home_team_score: str
    home_team_record: str
    home_passing_leader: str
    home_rushing_leader: str
    home_receiving_leader: str
    # home_passing_display_value: str
    # home_passing_value: float
    # home_passing_athlete_name: str
    # home_passing_athlete_position: str
    # home_rushing_display_value: str
    # home_rushing_value: float
    # home_rushing_athlete_name: str
    # home_rushing_athlete_position: str
    # home_receive_display_value: str
    # home_receive_value: float
    # home_receive_athlete_name: str
    # home_receive_athlete_position: str
    home_team_linescores: List[float]
    away_team_name: str
    away_team_abbreviation: str
    away_team_score: str
    away_team_record: str
    away_passing_leader: str
    away_rushing_leader: str
    away_receiving_leader: str
    # away_passing_display_value: str
    # away_passing_value: float
    # away_passing_athlete_name: str
    # away_passing_athlete_position: str
    # away_rushing_display_value: str
    # away_rushing_value: str
    # away_rushing_athlete_name: str
    # away_rushing_athlete_position: str
    # away_receive_display_value: str
    # away_receive_value: float
    # away_receive_athlete_name: str
    # away_receive_athlete_position: str
    away_team_linescores: List[float]
    venue_name: str
    venue_city: str
    venue_state: str
    venue_indoor: bool
    broadcasts: List[str]
    timestamp: datetime


class NBAGameStats(BaseGameStats):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    game_id: str
    start_time: str
    status_state: str
    status_detail: str
    status_description: str
    period: int
    clock: str
    home_team_name: str
    home_team_abbrev: str
    home_team_score: str
    home_team_field_goals: str
    home_team_three_pointers: str
    home_team_free_throws: str
    home_team_rebounds: str
    home_team_assists: str
    home_team_steals: str
    home_team_blocks: str
    home_team_turnovers: str
    away_team_name: str
    away_team_abbrev: str
    away_team_score: str
    away_team_field_goals: str
    away_team_three_pointers: str
    away_team_free_throws: str
    away_team_rebounds: str
    away_team_assists: str
    away_team_steals: str
    away_team_blocks: str
    away_team_turnovers: str
    venue_name: str
    venue_city: str
    venue_state: str
    broadcasts: List[str]
    timestamp: datetime


class NHLGameStats(BaseGameStats):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    game_id: str
    start_time: str
    status_state: str
    status_detail: str
    status_description: str
    period: int
    clock: str
    home_team_name: str
    home_team_abbrev: str
    home_team_score: str
    home_team_saves: str
    home_team_save_pct: str
    home_team_goals: str
    home_team_assists: str
    home_team_points: str
    home_team_record: str
    away_team_name: str
    away_team_abbrev: str
    away_team_score: str
    away_team_saves: str
    away_team_save_pct: str
    away_team_goals: str
    away_team_assists: str
    away_team_points: str
    away_team_record: str
    venue_name: str
    venue_city: str
    venue_state: str
    venue_indoor: bool
    broadcasts: List[str]
    timestamp: datetime


class MLBGameStats(BaseGameStats):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    current_inning: Optional[int]
    inning_half: Optional[str]
    outs: Optional[int]
    bases_occupied: List[int]
    score: Dict[str, int]
    stats: Dict[str, Dict[str, float]] = Field(
        description="Team statistics including hits, errors, etc."
    )
