from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
from betflow.historical.config import ProcessingConfig

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def validate_json_structure(sport_key, **context):
    """Validate required fields and data types in raw JSON"""
    s3_client = boto3.client("s3")
    date_str = context["ds"]

    try:
        response = s3_client.get_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=f"historical/games/{sport_key}/{date_str}/games.json",
        )
        data = json.loads(response["Body"].read().decode("utf-8"))

        # Game level validations
        for game in data:
            validate_game_data(game)
            validate_team_data(game)
            validate_venue_data(game)

        return True

    except Exception as e:
        print(f"Validation failed: {str(e)}")
        raise


def validate_game_data(game):
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


for sport in ["nba", "nfl", "cfb", "nhl"]:
    with DAG(
        f"{sport}_games_quality_check",
        default_args={
            **default_args,
            "start_date": ProcessingConfig.SPORT_CONFIGS[sport]["start_date"],
            "end_date": ProcessingConfig.SPORT_CONFIGS[sport]["end_date"],
        },
        description=f"Validate {sport.upper()} games data quality",
        schedule_interval="@daily",
        catchup=True,
    ) as dag:
        validate_source = PythonOperator(
            task_id=f"validate_{sport}_games",
            python_callable=validate_json_structure,
            op_kwargs={"sport_key": sport},
            provide_context=True,
        )

    globals()[f"{sport}_quality_dag"] = dag
