########################################
# Not using this DAG for now
########################################

from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.operators.bash import BashOperator
# from airflow.providers.amazon.aws.transfers.local_to_s3 import (
#     LocalFilesystemToS3Operator,
# )
import boto3

import asyncio
from datetime import datetime, timedelta
import json
import os
import aiohttp
from betflow.historical.hist_api_connectors import NBAHistoricalConnector
from betflow.historical.config import HistoricalConfig
from dotenv import load_dotenv

load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": True,  # Enable sequential processing
    "start_date": datetime(2024, 11, 27),  # NBA season 2025 start date is 2024-10-22
    "end_date": datetime(2024, 12, 1),  # Today's date
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # "dagrun_timeout": timedelta(minutes=60),  # Add timeout
    "tags": ["nba", "historical", "games"],  # Add tags
}


def fetch_games_by_date(**context):
    async def _fetch():
        """Fetch games and their statistics for a specific date"""
        logical_date = context.get("data_interval_start", context.get("logical_date"))
        if not logical_date:
            raise ValueError("No execution date found in context")
        date_str = logical_date.strftime("%Y-%m-%d")

        async with aiohttp.ClientSession() as session:
            games_connector = NBAHistoricalConnector(
                api_key=os.getenv("API_SPORTS_IO_KEY")
            )

            # First fetch games for the date
            conn_response = await games_connector.fetch_games_by_date(
                session, "standard", 2024, date_str
            )
            games_data = conn_response["response"]
            complete_games_data = []
            for game in games_data:
                game_id = str(game["id"])
                game_stats = await games_connector.fetch_game_statistics(
                    session, game_id
                )

                # Process and combine game and statistics data
                processed_game = games_connector.process_game_data(game, game_stats)
                complete_games_data.append(processed_game)

            if complete_games_data:
                context["task_instance"].xcom_push(
                    key="games_data", value=complete_games_data
                )
                context["task_instance"].xcom_push(key="date", value=date_str)

            return len(complete_games_data)

    # Run async code in sync context
    return asyncio.run(_fetch())


def process_games_data(**context):
    """Store processed games data to S3"""
    games_data = context["task_instance"].xcom_pull(key="games_data")
    date_str = context["task_instance"].xcom_pull(key="date")

    if not games_data:
        print("No games data found for processing")
        return None

    # Use config for output paths
    output_dir = f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{date_str}"
    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/games.json", "w") as f:
        json.dump(games_data, f)

    # for historical odds DAG
    # context["task_instance"].xcom_push(key="games_data", value=games_data)

    return output_dir


def upload_to_s3_func(**context):
    date_str = context["ds"]
    local_path = (
        f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{date_str}/games.json"
    )
    s3_path = f"{HistoricalConfig.S3_PATHS['games_prefix']}/nba/{date_str}/games.json"

    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(
            local_path,
            HistoricalConfig.S3_PATHS["raw_bucket"],
            s3_path,
            ExtraArgs={
                "ServerSideEncryption": "AES256"
            },  # Add encryption if bucket requires it
        )
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        raise


with DAG(
    "io_historical_games_ingestion",
    default_args=default_args,
    description="Ingest historical games data with backfill support",
    schedule_interval="@daily",
    catchup=True,  # Enable backfilling
) as dag:
    fetch_daily_games = PythonOperator(
        task_id="fetch_daily_games",
        python_callable=fetch_games_by_date,
        provide_context=True,
    )

    process_daily_games = PythonOperator(
        task_id="process_daily_games",
        python_callable=process_games_data,
        provide_context=True,
    )

    # upload_to_s3 = LocalFilesystemToS3Operator(
    #     task_id="upload_to_s3",
    #     filename=f"/tmp/{HistoricalConfig.S3_PATHS['games_prefix']}/{{{{ ds }}}}/games.json",
    #     dest_key=f"{HistoricalConfig.S3_PATHS['games_prefix']}/nba/{{{{ ds }}}}/games.json",
    #     dest_bucket=HistoricalConfig.S3_PATHS["raw_bucket"],
    #     # aws_conn_id=None,  # "aws_default",
    #     replace=True,
    # )
    # upload_to_s3 = BashOperator(
    #     task_id="upload_to_s3",
    #     bash_command=f'aws s3 sync /tmp/{HistoricalConfig.S3_PATHS["games_prefix"]}/{{{{ ds }}}}/games.json '
    #     f's3://{HistoricalConfig.S3_PATHS["raw_bucket"]}/{HistoricalConfig.S3_PATHS["games_prefix"]}/nba/{{{{ ds }}}}/games.json',
    # )
    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3", python_callable=upload_to_s3_func, provide_context=True
    )

    fetch_daily_games >> process_daily_games >> upload_to_s3
