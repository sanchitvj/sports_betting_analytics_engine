from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import sys
from datetime import timedelta, datetime
import os
import asyncio
import aiohttp
import json
import boto3
from betflow.historical.hist_api_connectors import HistoricalOddsConnector
from betflow.historical.config import HistoricalConfig
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(".env"))
load_dotenv(find_dotenv("my.env"), override=True)
# commented because weird issue where astro dev restart not updating code if using 2 env files


def fetch_odds_by_date(sport_key, **context):
    """Fetch odds data for games on a specific date"""

    async def _fetch():
        logical_date = context.get("data_interval_start") - timedelta(days=1)
        date_str = logical_date.strftime("%Y-%m-%d")
        # Get completed games for the date from previous games DAG
        games_data = context["task_instance"].xcom_pull(
            dag_id=f"historical_{sport_key}_games_ingestion",
            task_ids=f"fetch_daily_{sport_key}_games",
            key=f"{sport_key}_games_data",
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
                    session, sport_key, date_str
                )

                if odds_data:
                    # Write to temporary location
                    output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{sport_key}/{date_str}"
                    os.makedirs(output_dir, exist_ok=True)

                    with open(f"{output_dir}/odds.json", "w") as f:
                        json.dump(odds_data, f)

                    return odds_data
                return []

        except Exception as e:
            print(f"Error fetching odds: {str(e)}")
            raise AirflowException(f"Failed to fetch odds: {str(e)}")

    return asyncio.run(_fetch())


def upload_to_s3_func(sport_key, **context):
    date_str = context["ds"]
    date_frmt = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    date_str = date_frmt.strftime("%Y-%m-%d")

    local_path = f"/tmp/{HistoricalConfig.S3_PATHS['odds_prefix']}/{sport_key}/{date_str}/odds.json"
    s3_path = (
        f"{HistoricalConfig.S3_PATHS['odds_prefix']}/{sport_key}/{date_str}/odds.json"
    )

    if not os.path.exists(local_path):
        print(f"No odds data file found for {sport_key} on {date_str}, skipping upload")
        # return 0
        sys.exit(1)

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


for sport, config in HistoricalConfig.SPORT_CONFIGS.items():
    with DAG(
        f"historical_{sport}_odds_ingestion",
        default_args={
            "owner": "PENGUIN_DB",
            "depends_on_past": True,
            "start_date": config["start_date"],
            "end_date": config["start_date"],  # datetime(2024, 12, 1),  # Today's date
            "email_on_failure": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
            "tags": [sport, "historical", "odds"],
        },
        description="Ingest historical odds data with backfill support",
        schedule_interval="@daily",
        catchup=True,
    ) as dag:
        fetch_daily_odds = PythonOperator(
            task_id=f"fetch_daily_{sport}_odds",
            python_callable=fetch_odds_by_date,
            op_kwargs={"sport_key": sport},
            provide_context=True,
        )

        upload_to_s3 = PythonOperator(
            task_id="upload_to_s3",
            python_callable=upload_to_s3_func,
            op_kwargs={"sport_key": sport},
            provide_context=True,
        )

        fetch_daily_odds >> upload_to_s3

    globals()[f"{sport}_odds_dag"] = dag
