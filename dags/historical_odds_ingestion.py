from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from datetime import timedelta
import os
import asyncio
import aiohttp
import json
import boto3
from betflow.historical.hist_api_connectors import HistoricalOddsConnector
from betflow.historical.config import HistoricalConfig
from dotenv import load_dotenv

load_dotenv()


default_args = {
    "owner": "PENGUIN_DB",
    "depends_on_past": True,
    "start_date": HistoricalConfig.SPORT_CONFIGS["nba"][
        "start_date"
    ],  # datetime(2024, 11, 29),  # NBA season 2025 start date is 2024-10-22
    "end_date": HistoricalConfig.SPORT_CONFIGS["nba"][
        "end_date"
    ],  # datetime(2024, 12, 1),  # Today's date
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["nba", "historical", "odds"],
}


def fetch_odds_by_date(**context):
    """Fetch odds data for games on a specific date"""

    async def _fetch():
        logical_date = context.get("data_interval_start")
        date_str = logical_date.strftime("%Y-%m-%d")

        # Get completed games for the date from previous games DAG
        games_data = context["task_instance"].xcom_pull(
            dag_id="historical_nba_games_ingestion",
            task_ids="fetch_daily_nba_games",
            key="nba_games_data",
        )

        if not games_data:
            print(f"No completed games found for date {date_str}")
            return []

        # Process odds for completed games
        # all_odds_data = []
        try:
            odds_connector = HistoricalOddsConnector(api_key=os.getenv("ODDS_API_KEY"))

            async with aiohttp.ClientSession() as session:
                # Fetch all odds for the date in one request
                odds_data = await odds_connector.fetch_odds_by_date(
                    session, "nba", date_str
                )

                if odds_data:
                    # Write to temporary location
                    output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{logical_date.strftime('%Y-%m-%d')}"
                    os.makedirs(output_dir, exist_ok=True)

                    with open(f"{output_dir}/odds.json", "w") as f:
                        json.dump(odds_data, f)

                    return odds_data
                return []

        except Exception as e:
            print(f"Error fetching odds: {str(e)}")
            raise AirflowException(f"Failed to fetch odds: {str(e)}")

    return asyncio.run(_fetch())


def upload_to_s3_func(**context):
    date_str = context["ds"]
    local_path = f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{date_str}/odds.json"
    s3_path = f"{HistoricalConfig.S3_PATHS['odds_prefix']}/nba/{date_str}/odds.json"

    if not os.path.exists(local_path):
        print(f"No odds data file found for {date_str}, skipping upload")
        return 0

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_path,
            HistoricalConfig.S3_PATHS["raw_bucket"],
            s3_path,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        return 1
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise


with DAG(
    "historical_odds_ingestion",
    default_args=default_args,
    description="Ingest historical odds data with backfill support",
    schedule_interval="@daily",
    catchup=True,
) as dag:
    fetch_daily_odds = PythonOperator(
        task_id="fetch_daily_odds",
        python_callable=fetch_odds_by_date,
        provide_context=True,
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3", python_callable=upload_to_s3_func, provide_context=True
    )

    fetch_daily_odds >> upload_to_s3
