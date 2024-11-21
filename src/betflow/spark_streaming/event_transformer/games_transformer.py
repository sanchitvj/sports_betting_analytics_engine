from typing import Dict, Any
from datetime import datetime
from betflow.kafka_orch.schemas import MLBGameStats
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr


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


class GameTransformer(BaseGameTransformer):
    """Transform NBA game data from Kafka messages according to kafka schema."""

    @staticmethod
    def transform_espn_cfb(df: DataFrame) -> DataFrame:
        """Transform streaming CFB DataFrame.

        Args:
            df: Input DataFrame with parsed ESPN CFB data

        Returns:
            DataFrame: Transformed game data
        """
        try:
            return df.select(
                # Game identification
                col("game_id"),
                col("start_time"),
                # Game status
                col("status_state"),
                col("status_detail"),
                col("status_description"),
                col("period"),
                col("clock"),
                # Home team
                col("home_team_name"),
                col("home_team_abbrev"),
                col("home_team_score"),
                col("home_team_record"),
                # Home team statistics
                col("home_passing_leader"),
                col("home_rushing_leader"),
                col("home_receiving_leader"),
                # Away team
                col("away_team_name"),
                col("away_team_abbrev"),
                col("away_team_score"),
                col("away_team_record"),
                # Away team statistics
                col("away_passing_leader"),
                col("away_rushing_leader"),
                col("away_receiving_leader"),
                # Venue information
                col("venue_name"),
                col("venue_city"),
                col("venue_state"),
                # Additional information
                col("broadcasts"),
                # col("odds"),
                col("timestamp").alias("processing_time"),
            )

        except Exception as e:
            raise ValueError(f"Failed to transform ESPN CFB data: {e}")

    def transform_espn_nfl(self, df: DataFrame) -> DataFrame:
        """Transform streaming NFL DataFrame.

        Args:
            df: Input DataFrame with parsed ESPN NFL data

        Returns:
            DataFrame: Transformed game data
        """
        try:
            return df.select(
                # Game identification
                col("game_id"),
                col("start_time"),
                # Game status
                col("status.state").alias("status_state"),
                col("status.detail").alias("status_detail"),
                col("status.description").alias("status_description"),
                col("period"),
                col("clock"),
                # Home team
                col("home_team.name").alias("home_team_name"),
                col("home_team.abbreviation").alias("home_team_abbrev"),
                col("home_team.score").alias("home_team_score"),
                # Home team statistics
                expr("home_team.statistics[0].displayValue").alias(
                    "home_passing_yards"
                ),
                expr("home_team.statistics[1].displayValue").alias(
                    "home_rushing_yards"
                ),
                expr("home_team.statistics[2].displayValue").alias("home_total_yards"),
                expr("home_team.statistics[3].displayValue").alias(
                    "home_completion_pct"
                ),
                expr("home_team.statistics[4].displayValue").alias("home_third_down"),
                expr("home_team.statistics[5].displayValue").alias("home_fourth_down"),
                expr("home_team.statistics[6].displayValue").alias("home_sacks"),
                expr("home_team.statistics[7].displayValue").alias("home_turnovers"),
                # Away team
                col("away_team.name").alias("away_team_name"),
                col("away_team.abbreviation").alias("away_team_abbrev"),
                col("away_team.score").alias("away_team_score"),
                # Away team statistics
                expr("away_team.statistics[0].displayValue").alias(
                    "away_passing_yards"
                ),
                expr("away_team.statistics[1].displayValue").alias(
                    "away_rushing_yards"
                ),
                expr("away_team.statistics[2].displayValue").alias("away_total_yards"),
                expr("away_team.statistics[3].displayValue").alias(
                    "away_completion_pct"
                ),
                expr("away_team.statistics[4].displayValue").alias("away_third_down"),
                expr("away_team.statistics[5].displayValue").alias("away_fourth_down"),
                expr("away_team.statistics[6].displayValue").alias("away_sacks"),
                expr("away_team.statistics[7].displayValue").alias("away_turnovers"),
                # Game leaders
                expr("home_team.leaders[0].leaders[0].displayValue").alias(
                    "home_passing_leader"
                ),
                expr("home_team.leaders[1].leaders[0].displayValue").alias(
                    "home_rushing_leader"
                ),
                expr("home_team.leaders[2].leaders[0].displayValue").alias(
                    "home_receiving_leader"
                ),
                expr("away_team.leaders[0].leaders[0].displayValue").alias(
                    "away_passing_leader"
                ),
                expr("away_team.leaders[1].leaders[0].displayValue").alias(
                    "away_rushing_leader"
                ),
                expr("away_team.leaders[2].leaders[0].displayValue").alias(
                    "away_receiving_leader"
                ),
                # Venue information
                col("venue.name").alias("venue_name"),
                col("venue.city").alias("venue_city"),
                col("venue.state").alias("venue_state"),
                # Broadcasts and timestamp
                col("broadcasts"),
                col("timestamp").alias("processing_time"),
            )

        except Exception as e:
            raise ValueError(f"Failed to transform ESPN NFL data: {e}")

    def transform_espn_nhl(self, df: DataFrame) -> DataFrame:
        """Transform streaming NHL DataFrame.

        Args:
            df: Input DataFrame with parsed ESPN NHL data

        Returns:
            DataFrame: Transformed game data
        """
        try:
            return df.select(
                # Game identification
                col("game_id"),
                col("start_time"),
                # Game status
                col("status_state"),
                col("status_detail"),
                col("status_description"),
                col("period"),
                col("clock"),
                # Home team
                col("home_team_name"),
                col("home_team_abbrev"),
                col("home_team_score"),
                # Home team statistics
                col("home_team_saves").alias("home_saves"),
                col("home_team_save_pct").alias("home_save_pct"),
                col("home_team_goals").alias("home_goals"),
                col("home_team_assists").alias("home_assists"),
                col("home_team_points").alias("home_points"),
                # col("home_team_penalties").alias("home_penalties"),
                # col("home_team_penalty_minutes").alias("home_penalty_minutes"),
                # col("home_team_power_plays").alias("home_power_plays"),
                # col("home_team_power_play_goals").alias("home_power_play_goals"),
                # col("home_team_power_play_pct").alias("home_power_play_pct"),
                col("home_team_record").alias("home_team_record"),
                # Away team
                col("away_team_name"),
                col("away_team_abbrev"),
                col("away_team_score"),
                # Away team statistics
                col("away_team_saves").alias("away_saves"),
                col("away_team_save_pct").alias("away_save_pct"),
                col("away_team_goals").alias("away_goals"),
                col("away_team_assists").alias("away_assists"),
                col("away_team_points").alias("away_points"),
                # col("away_team_penalties").alias("away_penalties"),
                # col("away_team_penalty_minutes").alias("away_penalty_minutes"),
                # col("away_team_power_plays").alias("away_power_plays"),
                # col("away_team_power_play_goals").alias("away_power_play_goals"),
                # col("away_team_power_play_pct").alias("away_power_play_pct"),
                col("away_team_record").alias("away_team_record"),
                # Venue information
                col("venue_name"),
                col("venue_city"),
                col("venue_state"),
                col("venue_indoor"),
                # Broadcasts and timestamp
                col("broadcasts"),
                col("timestamp").alias("processing_time"),
            )

        except Exception as e:
            raise ValueError(f"Failed to transform ESPN NHL data: {e}")

    def transform_espn_nba(self, df: DataFrame) -> DataFrame:
        """Transform streaming NBA DataFrame.

        Args:
            df: Input DataFrame with parsed ESPN NBA data

        Returns:
            DataFrame: Transformed game data
        """
        try:
            # Create UDF for getting statistics

            return df.select(
                # Game identification
                col("game_id"),
                col("start_time"),
                # Game status
                col("status_state"),
                col("status_detail"),
                col("status_description"),
                col("period"),
                col("clock"),
                # Home team
                col("home_team_name"),
                col("home_team_abbrev"),
                col("home_team_score"),
                # Home team statistics
                col("home_team_field_goals").alias("home_fg_pct"),
                col("home_team_three_pointers").alias("home_three_pt_pct"),
                col("home_team_free_throws").alias("home_ft_pct"),
                col("home_team_rebounds").alias("home_rebounds"),
                col("home_team_assists").alias("home_assists"),
                col("home_team_steals").alias("home_steals"),
                col("home_team_blocks").alias("home_blocks"),
                col("home_team_turnovers").alias("home_turnovers"),
                # Away team
                col("away_team_name"),
                col("away_team_abbrev"),
                col("away_team_score"),
                # Away team statistics
                col("away_team_field_goals").alias("away_fg_pct"),
                col("away_team_three_pointers").alias("away_three_pt_pct"),
                col("away_team_free_throws").alias("away_ft_pct"),
                col("away_team_rebounds").alias("away_rebounds"),
                col("away_team_assists").alias("away_assists"),
                col("away_team_steals").alias("away_steals"),
                col("away_team_blocks").alias("away_blocks"),
                col("away_team_turnovers").alias("away_turnovers"),
                # Venue information
                col("venue_name"),
                col("venue_city"),
                col("venue_state"),
                # Add processing timestamp
                col("broadcasts"),
                col("timestamp").alias("processing_time"),
            )

        except Exception as e:
            raise ValueError(f"Failed to transform ESPN NBA data: {e}")
