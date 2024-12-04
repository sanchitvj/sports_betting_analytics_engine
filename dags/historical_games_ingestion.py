from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import json
import os
import boto3
from betflow.historical.hist_api_connectors import ESPNBatchConnector
from betflow.historical.config import HistoricalConfig
from betflow.api_connectors.raw_game_transformers import (
    api_raw_nba_data,
    api_raw_nfl_data,
    api_raw_nhl_data,
    api_raw_cfb_data,
)


def fetch_games_by_date(sport_key, **context):
    """Fetch games for a specific date and sport"""
    logical_date = context.get("data_interval_start") - timedelta(days=1)
    date_str = logical_date.strftime("%Y%m%d")

    try:
        espn_connector = ESPNBatchConnector()
        endpoint = HistoricalConfig.SPORT_CONFIGS[sport_key]["endpoint"]
        params = {"dates": date_str}

        raw_data = espn_connector.make_request(endpoint, params)

        # Check if events exist
        if not raw_data.get("events"):
            print(f"No games found for date {date_str}")
            return 0  # Return success but zero games

        processed_games = []
        for game in raw_data.get("events", []):
            if sport_key == "nba":
                game_data = api_raw_nba_data(game)
            elif sport_key == "nfl":
                game_data = api_raw_nfl_data(game)
            elif sport_key == "nhl":
                game_data = api_raw_nhl_data(game)
            elif sport_key == "cfb":
                game_data = api_raw_cfb_data(game)

            if game_data["status_state"] in ["post", "STATUS_FINAL"]:
                processed_games.append(game_data)

        if processed_games:
            output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{sport_key}/{logical_date.strftime('%Y-%m-%d')}"
            os.makedirs(output_dir, exist_ok=True)

            with open(f"{output_dir}/games.json", "w") as f:
                json.dump(processed_games, f)

            context["task_instance"].xcom_push(
                key=f"{sport_key}_games_data", value=processed_games
            )
            return len(processed_games)
        return 0

    except Exception as e:
        print(f"Error fetching {sport_key} games for date {date_str}: {str(e)}")
        raise


def upload_to_s3_func(sport_key, **context):
    """Upload processed games data to S3"""
    date_str = context["ds"] - timedelta(days=1)
    local_path = f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{sport_key}/{date_str}/games.json"
    s3_path = (
        f"{HistoricalConfig.S3_PATHS['games_prefix']}/{sport_key}/{date_str}/games.json"
    )

    if not os.path.exists(local_path):
        print(f"No data file found for {sport_key} on {date_str}, skipping upload")
        return 0

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_path, HistoricalConfig.S3_PATHS["raw_bucket"], s3_path
        )
        return 1
    except Exception as e:
        print(f"Error uploading {sport_key} data to S3: {str(e)}")
        raise


# Create DAGs for each sport
for sport_key, config in HistoricalConfig.SPORT_CONFIGS.items():
    with DAG(
        f"historical_{sport_key}_games_ingestion",
        default_args={
            "owner": "PENGUIN_DB",
            "depends_on_past": True,
            "start_date": config["start_date"],
            # "end_date": config["end_date"],
            "email_on_failure": False,
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "tags": [sport_key, "historical", "games"],
        },
        description=f"Ingest historical {sport_key.upper()} games data with backfill support",
        schedule_interval="@daily",
        catchup=True,
    ) as dag:
        fetch_daily_games = PythonOperator(
            task_id=f"fetch_daily_{sport_key}_games",
            python_callable=fetch_games_by_date,
            op_kwargs={"sport_key": sport_key},
            provide_context=True,
        )

        upload_to_s3 = PythonOperator(
            task_id=f"upload_{sport_key}_to_s3",
            python_callable=upload_to_s3_func,
            op_kwargs={"sport_key": sport_key},
            provide_context=True,
        )

        fetch_daily_games >> upload_to_s3
