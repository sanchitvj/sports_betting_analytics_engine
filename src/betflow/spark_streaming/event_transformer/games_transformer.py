from typing import Dict, Any
from datetime import datetime
from betflow.kafka_orch.schemas import NFLGameStats, NBAGameStats, MLBGameStats


class BaseGameTransformer:
    """Base transformer for game data."""

    @staticmethod
    def _extract_base_fields(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract common fields for all game types."""
        return {
            "game_id": raw_data["id"],
            "sport_type": raw_data.get("sport", ""),
            "start_time": datetime.fromisoformat(raw_data["start_time"]),
            "venue_id": raw_data["venue"]["id"],
            "status": raw_data["status"],
            "home_team_id": raw_data["home_team"]["id"],
            "away_team_id": raw_data["away_team"]["id"],
            "season": raw_data["season"]["year"],
            "season_type": raw_data["season"]["type"],
            "broadcast": raw_data.get("broadcast", []),
        }


class MLBGameTransformer(BaseGameTransformer):
    """Transform MLB game data."""

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw MLB game data to standardized format."""
        base_data = self._extract_base_fields(raw_data)

        game_data = {
            **base_data,
            "current_inning": raw_data.get("inning"),
            "inning_half": raw_data.get("inning_half"),
            "outs": raw_data.get("outs"),
            "bases_occupied": raw_data.get("bases_occupied", []),
            "score": {
                raw_data["home_team"]["id"]: raw_data["home_team"]["score"],
                raw_data["away_team"]["id"]: raw_data["away_team"]["score"],
            },
            "stats": self._transform_mlb_stats(raw_data.get("stats", {})),
        }

        # Validate against schema
        return MLBGameStats(**game_data).model_dump()

    @staticmethod
    def _transform_mlb_stats(stats: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        """Transform MLB-specific statistics."""
        return {
            "batting": {
                "hits": float(stats.get("hits", 0)),
                "runs": float(stats.get("runs", 0)),
                "errors": float(stats.get("errors", 0)),
                "at_bats": float(stats.get("at_bats", 0)),
            },
            "pitching": {
                "strikeouts": float(stats.get("strikeouts", 0)),
                "walks": float(stats.get("walks", 0)),
                "earned_runs": float(stats.get("earned_runs", 0)),
                "pitches": float(stats.get("pitches", 0)),
            },
        }


class NFLGameTransformer(BaseGameTransformer):
    """Transform NFL game data."""

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw NFL game data to standardized format."""
        base_data = self._extract_base_fields(raw_data)

        game_data = {
            **base_data,
            "current_quarter": raw_data.get("quarter"),
            "time_remaining": raw_data.get("time_remaining"),
            "down": raw_data.get("down"),
            "yards_to_go": raw_data.get("yards_to_go"),
            "possession": raw_data.get("possession"),
            "score": {
                raw_data["home_team"]["id"]: raw_data["home_team"]["score"],
                raw_data["away_team"]["id"]: raw_data["away_team"]["score"],
            },
            "stats": self._transform_nfl_stats(raw_data.get("stats", {})),
        }

        # Validate against schema
        return NFLGameStats(**game_data).model_dump()

    @staticmethod
    def _transform_nfl_stats(stats: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        """Transform NFL-specific statistics."""
        return {
            "passing": {
                "yards": float(stats.get("passing_yards", 0)),
                "touchdowns": float(stats.get("passing_touchdowns", 0)),
                "completions": float(stats.get("completions", 0)),
            },
            "rushing": {
                "yards": float(stats.get("rushing_yards", 0)),
                "touchdowns": float(stats.get("rushing_touchdowns", 0)),
                "attempts": float(stats.get("rushing_attempts", 0)),
            },
        }


class NBAGameTransformer(BaseGameTransformer):
    """Transform NBA game data."""

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw NBA game data to standardized format."""
        base_data = self._extract_base_fields(raw_data)

        game_data = {
            **base_data,
            "current_period": raw_data.get("period"),
            "time_remaining": raw_data.get("time_remaining"),
            "score": {
                raw_data["home_team"]["id"]: raw_data["home_team"]["score"],
                raw_data["away_team"]["id"]: raw_data["away_team"]["score"],
            },
            "stats": self._transform_nba_stats(raw_data.get("stats", {})),
        }

        # Validate against schema
        return NBAGameStats(**game_data).model_dump()

    @staticmethod
    def _transform_nba_stats(stats: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        """Transform NBA-specific statistics."""
        return {
            "shooting": {
                "field_goals": float(stats.get("field_goals", 0)),
                "three_pointers": float(stats.get("three_pointers", 0)),
                "free_throws": float(stats.get("free_throws", 0)),
            },
            "rebounds": {
                "offensive": float(stats.get("offensive_rebounds", 0)),
                "defensive": float(stats.get("defensive_rebounds", 0)),
                "total": float(stats.get("total_rebounds", 0)),
            },
        }
