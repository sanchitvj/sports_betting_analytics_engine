import pytest
from datetime import datetime
from betflow.spark_streaming.event_transformer import OddsTransformer


class TestOddsTransformer:
    @pytest.fixture
    def odds_transformer(self):
        """Create OddsTransformer instance."""
        return OddsTransformer()

    @pytest.fixture
    def odds_api_data(self):
        """Fixture for The Odds API data."""
        return {
            "id": "game123",
            "bookmaker_key": "betmgm",
            "bookmaker_name": "BetMGM",
            "market_key": "h2h",
            "market_name": "Moneyline",
            "last_update": int(datetime.now().timestamp()),
            "price": -110,
            "opening_price": -105,
            "status": "active",
            "home_team": "Lakers",
            "away_team": "Celtics",
            "volume": 100000,
            "matched": 95000,
        }

    @pytest.fixture
    def pinnacle_data(self):
        """Fixture for Pinnacle data."""
        return {
            "eventId": "event123",
            "leagueId": "league123",
            "lastUpdate": int(datetime.now().timestamp()),
            "betType": "moneyline",
            "price": 1.91,
            "opening_price": 1.95,
            "spread": -3.5,
            "total": 220.5,
            "volume": 50000,
            "currency": "USD",
            "status": "active",
            "periodNumber": 0,
            "teamType": "home",
            "lineId": "line123",
        }

    def test_transform_odds_api_valid_data(self, odds_transformer, odds_api_data):
        """Test The Odds API data transformation with valid data."""
        sport_type = "NBA"
        result = odds_transformer.transform_odds_api(odds_api_data, sport_type)

        # Verify basic fields
        assert isinstance(result["odds_id"], str)
        assert result["game_id"] == odds_api_data["id"]
        assert result["sport_type"] == sport_type
        assert result["bookmaker_id"] == odds_api_data["bookmaker_key"]
        assert isinstance(result["timestamp"], datetime)
        assert result["market_type"] == odds_api_data["market_key"]
        assert result["odds_value"] == float(odds_api_data["price"])
        assert result["status"] == odds_api_data["status"]

        # Verify movement data
        assert result["movement"]["opening"] == float(odds_api_data["opening_price"])
        assert result["movement"]["current"] == float(odds_api_data["price"])

        # Verify volume data
        assert result["volume"]["total"] == float(odds_api_data["volume"])
        assert result["volume"]["matched"] == float(odds_api_data["matched"])

        # Verify metadata
        assert result["metadata"]["bookmaker_name"] == odds_api_data["bookmaker_name"]
        assert result["metadata"]["market_name"] == odds_api_data["market_name"]

    def test_transform_odds_api_missing_optional(self, odds_transformer, odds_api_data):
        """Test The Odds API transformation with missing optional fields."""
        # Remove optional fields
        del odds_api_data["opening_price"]
        del odds_api_data["volume"]
        del odds_api_data["matched"]

        result = odds_transformer.transform_odds_api(odds_api_data, "NBA")

        assert result["movement"]["opening"] == 0
        assert result["volume"] is None

    # def test_transform_pinnacle_valid_data(self, odds_transformer, pinnacle_data):
    #     """Test Pinnacle data transformation with valid data."""
    #     sport_type = "NBA"
    #     result = odds_transformer.transform_pinnacle(pinnacle_data, sport_type)
    #
    #     # Verify basic fields
    #     assert isinstance(result["odds_id"], str)
    #     assert result["game_id"] == str(pinnacle_data["eventId"])
    #     assert result["sport_type"] == sport_type
    #     assert result["bookmaker_id"] == "pinnacle"
    #     assert isinstance(result["timestamp"], datetime)
    #     assert result["market_type"] == pinnacle_data["betType"]
    #     assert result["odds_value"] == float(pinnacle_data["price"])
    #     assert result["spread_value"] == pinnacle_data["spread"]
    #     assert result["total_value"] == pinnacle_data["total"]
    #     assert result["status"] == pinnacle_data["status"]
    #
    #     # Verify movement data
    #     assert result["movement"]["opening"] == float(pinnacle_data["opening_price"])
    #     assert result["movement"]["current"] == float(pinnacle_data["price"])
    #
    #     # Verify volume data
    #     assert result["volume"]["amount"] == float(pinnacle_data["volume"])
    #     assert result["volume"]["currency"] == pinnacle_data["currency"]
    #
    #     # Verify metadata
    #     assert result["metadata"]["league_id"] == pinnacle_data["leagueId"]
    #     assert result["metadata"]["period_number"] == pinnacle_data["periodNumber"]
    #     assert result["metadata"]["line_id"] == pinnacle_data["lineId"]

    # def test_transform_pinnacle_missing_optional(self, odds_transformer, pinnacle_data):
    #     """Test Pinnacle transformation with missing optional fields."""
    #     # Remove optional fields
    #     del pinnacle_data["spread"]
    #     del pinnacle_data["total"]
    #     del pinnacle_data["opening_price"]
    #
    #     result = odds_transformer.transform_pinnacle(pinnacle_data, "NBA")
    #
    #     assert result["spread_value"] is None
    #     assert result["total_value"] is None
    #     assert result["movement"]["opening"] == 0

    def test_generate_odds_id(self, odds_transformer):
        """Test odds ID generation."""
        # Test uniqueness
        id1 = odds_transformer._generate_odds_id("game1", "book1", "market1")
        id2 = odds_transformer._generate_odds_id("game1", "book1", "market2")
        assert id1 != id2

        # Test consistency
        assert (
            odds_transformer._generate_odds_id("game1", "book1", "market1") != id1
        )  # Should be different due to timestamp

    def test_extract_odds_value(self, odds_transformer):
        """Test odds value extraction."""
        # Test with price
        assert odds_transformer._extract_odds_value({"price": 1.95}) == 1.95

        # Test with odds
        assert odds_transformer._extract_odds_value({"odds": -110}) == -110

        # Test with missing values
        assert odds_transformer._extract_odds_value({}) == 0.0

    def test_calculate_probability(self, odds_transformer):
        """Test probability calculation."""
        # Test positive American odds
        data = {"price": 200}
        prob = odds_transformer._calculate_probability(data)
        assert 0 < prob < 1

        # Test negative American odds
        data = {"price": -150}
        prob = odds_transformer._calculate_probability(data)
        assert 0 < prob < 1

        # Test missing odds
        assert odds_transformer._calculate_probability({}) is None

    def test_extract_volume_data(self, odds_transformer):
        """Test volume data extraction."""
        # Test with both volume types
        data = {"volume": 1000, "matched": 900}
        volume = odds_transformer._extract_volume_data(data)
        assert volume["total"] == 1000
        assert volume["matched"] == 900

        # Test with missing data
        assert odds_transformer._extract_volume_data({}) is None

    def test_error_handling(self, odds_transformer):
        """Test error handling in transformations."""
        # Test with invalid data
        with pytest.raises(ValueError):
            odds_transformer.transform_odds_api({}, "NBA")

        with pytest.raises(ValueError):
            odds_transformer.transform_pinnacle({}, "NBA")
