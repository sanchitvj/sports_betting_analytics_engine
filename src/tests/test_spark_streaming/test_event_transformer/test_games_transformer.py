import pytest
from datetime import datetime
from betflow.spark_streaming.event_transformer import (
    BaseGameTransformer,
    NFLGameTransformer,
    NBAGameTransformer,
    MLBGameTransformer,
)


@pytest.fixture
def base_game_data():
    """Fixture for base game data."""
    return {
        "id": "game123",
        "sport": "NFL",
        "start_time": datetime.now().isoformat(),
        "venue": {"id": "venue123"},
        "status": "in_progress",
        "home_team": {"id": "home123"},
        "away_team": {"id": "away123"},
        "season": {"year": 2024, "type": "regular"},
        "broadcast": ["ESPN", "FOX"],
    }


@pytest.fixture
def nfl_game_data(base_game_data):
    """Fixture for NFL game data."""
    return {
        **base_game_data,
        "quarter": 2,
        "time_remaining": "10:30",
        "down": 2,
        "yards_to_go": 8,
        "possession": "home123",
        "home_team": {"id": "home123", "score": 14},
        "away_team": {"id": "away123", "score": 7},
        "stats": {
            "passing_yards": 200,
            "passing_touchdowns": 2,
            "completions": 15,
            "rushing_yards": 100,
            "rushing_touchdowns": 1,
            "rushing_attempts": 20,
        },
    }


@pytest.fixture
def nba_game_data(base_game_data):
    """Fixture for NBA game data."""
    return {
        **base_game_data,
        "sport": "NBA",
        "period": 2,
        "time_remaining": "5:45",
        "home_team": {"id": "home123", "score": 52},
        "away_team": {"id": "away123", "score": 48},
        "stats": {
            "field_goals": 20,
            "three_pointers": 6,
            "free_throws": 6,
            "offensive_rebounds": 5,
            "defensive_rebounds": 15,
            "total_rebounds": 20,
        },
    }


@pytest.fixture
def mlb_game_data(base_game_data):
    """Fixture for MLB game data."""
    return {
        **base_game_data,
        "sport": "MLB",
        "inning": 5,
        "inning_half": "top",
        "outs": 2,
        "bases_occupied": [1, 3],
        "home_team": {"id": "home123", "score": 3},
        "away_team": {"id": "away123", "score": 2},
        "stats": {
            "hits": 8,
            "runs": 3,
            "errors": 1,
            "at_bats": 20,
            "strikeouts": 6,
            "walks": 3,
            "earned_runs": 2,
            "pitches": 75,
        },
    }


class TestBaseGameTransformer:
    def test_extract_base_fields(self, base_game_data):
        """Test extraction of base fields."""
        transformer = BaseGameTransformer()
        result = transformer._extract_base_fields(base_game_data)

        assert result["game_id"] == base_game_data["id"]
        assert result["sport_type"] == base_game_data["sport"]
        assert result["venue_id"] == base_game_data["venue"]["id"]
        assert result["home_team_id"] == base_game_data["home_team"]["id"]
        assert result["away_team_id"] == base_game_data["away_team"]["id"]
        assert result["season"] == base_game_data["season"]["year"]
        assert result["season_type"] == base_game_data["season"]["type"]
        assert result["broadcast"] == base_game_data["broadcast"]

    def test_extract_base_fields_missing_optional(self, base_game_data):
        """Test extraction with missing optional fields."""
        del base_game_data["broadcast"]
        transformer = BaseGameTransformer()
        result = transformer._extract_base_fields(base_game_data)

        assert result["broadcast"] == []


class TestNFLGameTransformer:
    def test_transform_valid_data(self, nfl_game_data):
        """Test NFL game data transformation."""
        transformer = NFLGameTransformer()
        result = transformer.transform(nfl_game_data)

        assert result["game_id"] == nfl_game_data["id"]
        assert result["sport_type"] == "NFL"
        assert result["current_quarter"] == nfl_game_data["quarter"]
        assert result["time_remaining"] == nfl_game_data["time_remaining"]
        assert result["down"] == nfl_game_data["down"]
        assert result["yards_to_go"] == nfl_game_data["yards_to_go"]
        assert result["possession"] == nfl_game_data["possession"]
        assert result["score"]["home123"] == nfl_game_data["home_team"]["score"]
        assert result["score"]["away123"] == nfl_game_data["away_team"]["score"]

        # Verify stats transformation
        assert result["stats"]["passing"]["yards"] == float(
            nfl_game_data["stats"]["passing_yards"]
        )
        assert result["stats"]["rushing"]["yards"] == float(
            nfl_game_data["stats"]["rushing_yards"]
        )

    def test_transform_missing_optional_fields(self, nfl_game_data):
        """Test NFL transformation with missing optional fields."""
        del nfl_game_data["time_remaining"]
        del nfl_game_data["stats"]["passing_touchdowns"]

        transformer = NFLGameTransformer()
        result = transformer.transform(nfl_game_data)

        assert result["time_remaining"] is None
        assert result["stats"]["passing"]["touchdowns"] == 0.0


class TestNBAGameTransformer:
    def test_transform_valid_data(self, nba_game_data):
        """Test NBA game data transformation."""
        transformer = NBAGameTransformer()
        result = transformer.transform(nba_game_data)

        assert result["game_id"] == nba_game_data["id"]
        assert result["sport_type"] == "NBA"
        assert result["current_period"] == nba_game_data["period"]
        assert result["time_remaining"] == nba_game_data["time_remaining"]
        assert result["score"]["home123"] == nba_game_data["home_team"]["score"]
        assert result["score"]["away123"] == nba_game_data["away_team"]["score"]

        # Verify stats transformation
        assert result["stats"]["shooting"]["field_goals"] == float(
            nba_game_data["stats"]["field_goals"]
        )
        assert result["stats"]["rebounds"]["total"] == float(
            nba_game_data["stats"]["total_rebounds"]
        )

    def test_transform_missing_optional_fields(self, nba_game_data):
        """Test NBA transformation with missing optional fields."""
        del nba_game_data["time_remaining"]
        del nba_game_data["stats"]["three_pointers"]

        transformer = NBAGameTransformer()
        result = transformer.transform(nba_game_data)

        assert result["time_remaining"] is None
        assert result["stats"]["shooting"]["three_pointers"] == 0.0


class TestMLBGameTransformer:
    def test_transform_valid_data(self, mlb_game_data):
        """Test MLB game data transformation."""
        transformer = MLBGameTransformer()
        result = transformer.transform(mlb_game_data)

        assert result["game_id"] == mlb_game_data["id"]
        assert result["sport_type"] == "MLB"
        assert result["current_inning"] == mlb_game_data["inning"]
        assert result["inning_half"] == mlb_game_data["inning_half"]
        assert result["outs"] == mlb_game_data["outs"]
        assert result["bases_occupied"] == mlb_game_data["bases_occupied"]
        assert result["score"]["home123"] == mlb_game_data["home_team"]["score"]
        assert result["score"]["away123"] == mlb_game_data["away_team"]["score"]

        # Verify stats transformation
        assert result["stats"]["batting"]["hits"] == float(
            mlb_game_data["stats"]["hits"]
        )
        assert result["stats"]["pitching"]["strikeouts"] == float(
            mlb_game_data["stats"]["strikeouts"]
        )

    def test_transform_missing_optional_fields(self, mlb_game_data):
        """Test MLB transformation with missing optional fields."""
        del mlb_game_data["inning_half"]
        del mlb_game_data["stats"]["walks"]

        transformer = MLBGameTransformer()
        result = transformer.transform(mlb_game_data)

        assert result["inning_half"] is None
        assert result["stats"]["pitching"]["walks"] == 0.0


def test_invalid_sport_type(base_game_data):
    """Test handling of invalid sport type."""
    base_game_data["sport"] = "INVALID"
    transformer = BaseGameTransformer()
    result = transformer._extract_base_fields(base_game_data)
    assert result["sport_type"] == "INVALID"
