from datetime import datetime
import boto3
import json
from betflow.historical.config import ProcessingConfig


def validate_sports_json_structure(sport_key, **context):
    """Validate required fields and data types in raw JSON"""
    s3_client = boto3.client("s3")
    date_str = context["ds"]
    date_frmt = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    date_str = date_frmt.strftime("%Y-%m-%d")

    try:
        response = s3_client.get_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=f"historical/games/{sport_key}/{date_str}/games.json",
        )
        data = json.loads(response["Body"].read().decode("utf-8"))

        # Game level validations
        for game in data:
            validate_sports_game_data(game)
            validate_team_data(game)
            validate_venue_data(game)

        return True

    except Exception as e:
        print(f"Validation failed: {str(e)}")
        raise


def validate_sports_game_data(game):
    """Validate individual game data"""
    required_fields = [
        "game_id",
        "start_time",
        "status_state",
        "status_detail",
        "status_description",
        "period",
        "clock",
    ]

    if not all(field in game for field in required_fields):
        raise ValueError(f"Game missing required fields: {required_fields}")

    # Validate timestamp format
    try:
        datetime.strptime(game["start_time"], "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise ValueError("Invalid start_time format")

    # Validate status values
    valid_states = ["pre", "in", "post"]
    if game["status_state"] not in valid_states:
        raise ValueError(f"Invalid status_state: {game['status_state']}")


def validate_team_data(game):
    """Validate team information"""
    team_fields = [
        "home_team_id",
        "home_team_name",
        "home_team_abbreviation",
        "home_team_score",
        "away_team_id",
        "away_team_name",
        "away_team_abbreviation",
        "away_team_score",
    ]

    if not all(field in game for field in team_fields):
        raise ValueError(f"Missing team fields: {team_fields}")

    # Validate score format
    try:
        int(game["home_team_score"])
        int(game["away_team_score"])
    except ValueError:
        raise ValueError("Invalid score format")


def validate_venue_data(game):
    """Validate venue information"""
    venue_fields = ["venue_name", "venue_city", "venue_state"]

    if not all(field in game for field in venue_fields):
        raise ValueError(f"Missing venue fields: {venue_fields}")


def validate_odds_json_structure(sport_key, **context):
    """Validate required fields and data types in raw JSON"""
    s3_client = boto3.client("s3")
    date_str = context["ds"]
    date_frmt = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    date_str = date_frmt.strftime("%Y-%m-%d")

    try:
        response = s3_client.get_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=f"historical/odds/{sport_key}/{date_str}/odds.json",
        )
        data = json.loads(response["Body"].read().decode("utf-8"))

        # Required root level fields
        required_fields = ["timestamp", "data"]
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")

        # Game level validations
        for game in data["data"]:
            validate_odds_game_data(game)

        # Bookmaker level validations
        for game in data["data"]:
            validate_bookmaker_data(game["bookmakers"])

        return True

    except Exception as e:
        print(f"Validation failed: {str(e)}")
        raise


def validate_odds_game_data(game):
    """Validate individual game data"""
    required_game_fields = [
        "id",
        "sport_key",
        "sport_title",
        "commence_time",
        "home_team",
        "away_team",
        "bookmakers",
    ]

    if not all(field in game for field in required_game_fields):
        raise ValueError(f"Game missing required fields: {required_game_fields}")

    if not isinstance(game["bookmakers"], list):
        raise ValueError("Bookmakers must be an array")

    # Validate timestamp format
    try:
        datetime.strptime(game["commence_time"], "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise ValueError("Invalid commence_time format")


def validate_bookmaker_data(bookmakers):
    """Validate bookmaker and odds data"""
    required_bookmaker_fields = ["key", "title", "last_update", "markets"]

    for bookmaker in bookmakers:
        if not all(field in bookmaker for field in required_bookmaker_fields):
            raise ValueError(
                f"Bookmaker missing required fields: {required_bookmaker_fields}"
            )

        for market in bookmaker["markets"]:
            if market["key"] != "h2h":
                continue

            if len(market["outcomes"]) != 2:
                raise ValueError("H2H market must have exactly 2 outcomes")

            for outcome in market["outcomes"]:
                if not isinstance(outcome["price"], (int, float)):
                    raise ValueError("Invalid price format")
