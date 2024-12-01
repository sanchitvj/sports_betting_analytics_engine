from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from datetime import datetime, timedelta
import json
import os
from betflow.historical.hist_api_connectors import HistoricalOddsConnector
from betflow.historical.config import HistoricalConfig


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2024, 10, 22),  # NBA season 2025 start date
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_games_for_date(**context):
    """Get finished games for the execution date"""
    execution_date = context["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")

    games_connector = HistoricalOddsConnector(api_key=os.getenv("API_SPORTS_IO_KEY"))
    games = games_connector.fetch_games_by_date(date_str)

    # Filter finished games using config status
    finished_games = [
        game
        for game in games
        if game["game"]["status"]["short"] in HistoricalConfig.FINISHED_STATUSES
    ]

    context["task_instance"].xcom_push(key="games", value=finished_games)
    return len(finished_games)


def fetch_odds_for_games(**context):
    """Fetch historical odds for games"""
    games = context["task_instance"].xcom_pull(key="games")
    date_str = context["execution_date"].strftime("%Y-%m-%d")

    odds_connector = HistoricalOddsConnector(api_key=os.getenv("ODDS_API_KEY"))
    all_odds = []

    for game in games:
        game_id = game["game"]["id"]
        game_time = datetime.fromtimestamp(game["game"]["date"]["timestamp"])
        game_end = game_time + timedelta(minutes=HistoricalConfig.GAME_DURATIONS["nfl"])

        # Get odds snapshots using config interval
        current_time = game_time
        while current_time <= game_end:
            odds = odds_connector.fetch_game_odds(
                game_id=game_id, timestamp=current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            if odds:
                all_odds.append(
                    {
                        "game_id": game_id,
                        "timestamp": current_time.isoformat(),
                        "odds_data": odds,
                    }
                )
            current_time += timedelta(minutes=HistoricalConfig.ODDS_INTERVAL)

        # Write to temporary location using config paths
    output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{date_str}"
    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/odds.json", "w") as f:
        json.dump(all_odds, f)

    return output_dir


with DAG(
    "historical_odds_ingestion",
    default_args=default_args,
    description="Ingest historical odds data with backfill support",
    schedule_interval="@daily",
    catchup=True,
) as dag:
    get_daily_games = PythonOperator(
        task_id="get_daily_games", python_callable=get_games_for_date
    )

    fetch_game_odds = PythonOperator(
        task_id="fetch_game_odds", python_callable=fetch_odds_for_games
    )

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{{{{ ds }}}}/*.json",
        dest_key=f"{HistoricalConfig.S3_PATHS['odds_prefix']}/sport=nfl/date={{{{ ds }}}}/odds.json",
        dest_bucket=HistoricalConfig.S3_PATHS["raw_bucket"],
        aws_conn_id="aws_default",
        replace=True,
    )

    get_daily_games >> fetch_game_odds >> upload_to_s3
