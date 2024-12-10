from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from betflow.historical.hist_utils import (
    fetch_odds_by_date,
    check_source_data,
    validate_upload_odds_json,
)
from betflow.historical.config import HistoricalConfig
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv("my.env"), override=True)
# do not uncomment and unpause DAG

default_args = {
    "owner": HistoricalConfig.OWNER,
    "depends_on_past": True,  # Make it False if you don't want to backfill sequentially
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["ingestion", "historical", "odds", "dqc"],
}


with DAG(
    "odds_ingestion_dqc",
    default_args=default_args,
    # start_date=datetime(2022, 8, 1),
    start_date=datetime(
        2024, 12, 6
    ),  # don't change only backfill for current season now
    # end_date=datetime(2022, 11, 27),
    # schedule_interval="@daily",
    schedule_interval="2 0 * * *",
    catchup=True,
    max_active_runs=16,  # Increase concurrent DAG runs
    concurrency=32,  # Increase task concurrency
) as dag:
    for sport_key, config in HistoricalConfig.SPORT_CONFIGS.items():
        with TaskGroup(f"{sport_key}_pipeline") as sport_group:
            check_data = ShortCircuitOperator(
                task_id=f"check_{sport_key}_data",
                python_callable=check_source_data,
                op_kwargs={"args": {"type": "games", "sport": sport_key}},
                provide_context=True,
            )

            fetch_odds = PythonOperator(
                task_id=f"fetch_{sport_key}_odds",
                python_callable=fetch_odds_by_date,
                op_kwargs={"sport_key": sport_key},
            )

            validate_data = PythonOperator(
                task_id=f"validate_{sport_key}_odds_json",
                python_callable=validate_upload_odds_json,
                op_kwargs={"sport_key": sport_key},
            )

            check_data >> fetch_odds >> validate_data
