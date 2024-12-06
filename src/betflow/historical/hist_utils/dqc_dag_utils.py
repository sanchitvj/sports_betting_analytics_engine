from datetime import datetime, timedelta
import boto3
import json
from betflow.historical.config import ProcessingConfig


def validate_upload_sports_json(sport_key, **context):
    """Validate JSON data before uploading to S3"""
    # Get data from XCom that was fetched in previous task
    games_data = context["task_instance"].xcom_pull(
        task_ids=f"{sport_key}_pipeline.fetch_{sport_key}_games",
        key=f"{sport_key}_games_data",
    )

    if not games_data:
        print(f"No games data found for {sport_key}")
        return True

    try:
        # Validate the data before upload
        for game in games_data:
            validate_sports_game_data(game)
            validate_team_data(game)
            validate_venue_data(game)

        # If validation passes, upload to S3
        date_str = (context["data_interval_start"] - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        s3_client = boto3.client("s3")
        s3_path = f"historical/games/{sport_key}/{date_str}/games.json"

        s3_client.put_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=s3_path,
            Body=json.dumps(games_data),
        )
        return True

    except Exception as e:
        print(f"Validation/Upload failed: {str(e)}")
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
        datetime.strptime(game["start_time"], "%Y-%m-%dT%H:%MZ")
    except ValueError:
        raise ValueError(f"Invalid start_time format, {game['start_time']}")

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


def validate_upload_odds_json(sport_key, **context):
    """Validate required fields and data types in raw JSON"""

    odds_data = context["task_instance"].xcom_pull(
        task_ids=f"{sport_key}_pipeline.fetch_{sport_key}_odds",
        key=f"{sport_key}_odds_data",
    )

    if not odds_data:
        print(f"No odds data found for {sport_key}")
        return True

    try:
        # Required root level fields
        required_fields = ["timestamp", "data"]
        if not all(field in odds_data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")

        # Game level validations
        for odds in odds_data["data"]:
            validate_odds_game_data(odds)
            validate_bookmaker_data(odds["bookmakers"])

        s3_client = boto3.client("s3")
        date_str = (context["data_interval_start"] - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        s3_path = f"historical/odds/{sport_key}/{date_str}/odds.json"
        s3_client.put_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=s3_path,
            Body=json.dumps(odds_data),
        )

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
