from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from betflow.historical.config import HistoricalConfig
from betflow.historical.hist_utils import (
    fetch_games_by_date,
    validate_upload_sports_json,
)

default_args = {
    "owner": HistoricalConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["backup", "ingestion", "historical", "sports", "dqc"],
}


with DAG(
    "backup_sports_ingestion_dqc",
    default_args=default_args,
    start_date=datetime(2022, 7, 31),
    end_date=datetime(2024, 12, 19),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=4,
    # concurrency=32,
) as dag:
    sports_to_process = ["nba", "nhl"]

    for sport in sports_to_process:
        with TaskGroup(f"{sport}_pipeline") as sport_group:
            # Fetch all seasons data at once
            fetch_games = PythonOperator(
                task_id=f"fetch_{sport}_games",
                python_callable=fetch_games_by_date,
                op_kwargs={"sport_key": sport},
            )

            validate_and_upload = PythonOperator(
                task_id=f"validate_{sport}_json",
                python_callable=validate_upload_sports_json,
                op_kwargs={"sport_key": sport},
            )

            fetch_games >> validate_and_upload
