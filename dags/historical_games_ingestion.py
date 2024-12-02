from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import boto3
from betflow.api_connectors import ESPNConnector
from betflow.historical.config import HistoricalConfig

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2024, 11, 29),
    "end_date": datetime(2024, 12, 1),
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["nba", "historical", "games"],  # Add tags
}


def fetch_games_by_date(**context):
    """Fetch games for a specific date"""
    logical_date = context.get("data_interval_start")
    date_str = logical_date.strftime("%Y%m%d")  # Format: YYYYMMDD for ESPN API

    try:
        espn_connector = ESPNConnector(historical=True)
        endpoint = "basketball/nba/scoreboard"
        params = {"dates": date_str}

        # Make rate-limited request
        raw_data = espn_connector.make_request(endpoint, params)

        # Process games using existing transformer
        processed_games = []
        for game in raw_data.get("events", []):
            game_data = espn_connector.api_raw_nba_data(game)
            if game_data["status_state"] in ["post", "STATUS_FINAL"]:
                processed_games.append(game_data)

        if processed_games:
            # Write to temporary location
            output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{logical_date.strftime('%Y-%m-%d')}"
            os.makedirs(output_dir, exist_ok=True)

            with open(f"{output_dir}/games.json", "w") as f:
                json.dump(processed_games, f)

            context["task_instance"].xcom_push(key="games_data", value=processed_games)
            return len(processed_games)
        return 0

    except Exception as e:
        print(f"Error fetching games for date {date_str}: {str(e)}")
        raise


def upload_to_s3_func(**context):
    """Upload processed games data to S3"""
    date_str = context["ds"]
    local_path = (
        f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{date_str}/games.json"
    )
    s3_path = f"{HistoricalConfig.S3_PATHS['games_prefix']}/nba/{date_str}/games.json"

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_path, HistoricalConfig.S3_PATHS["raw_bucket"], s3_path
        )
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise


with DAG(
    "historical_games_ingestion",
    default_args=default_args,
    description="Ingest historical NBA games data with backfill support",
    schedule_interval="@daily",
    catchup=True,
) as dag:
    fetch_daily_games = PythonOperator(
        task_id="fetch_daily_games",
        python_callable=fetch_games_by_date,
        provide_context=True,
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3", python_callable=upload_to_s3_func, provide_context=True
    )

    fetch_daily_games >> upload_to_s3
