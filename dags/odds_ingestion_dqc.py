from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime
from betflow.historical.hist_utils import (
    fetch_odds_by_date,
    validate_upload_odds_json,
)
from betflow.historical.config import HistoricalConfig
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv("my.env"), override=True)

default_args = {
    "owner": HistoricalConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["ingestion", "historical", "odds", "dqc"],
}


with DAG(
    "odds_ingestion_dqc",
    default_args=default_args,
    start_date=datetime(2022, 11, 27),
    end_date=datetime(2022, 11, 30),
    schedule_interval="@daily",
    catchup=True,
    # start_date=min(
    #     config["start_date"] for config in ProcessingConfig.SPORT_CONFIGS.values()
    # ),
) as dag:
    for sport_key, config in HistoricalConfig.SPORT_CONFIGS.items():
        with TaskGroup(f"{sport_key}_pipeline") as sport_group:
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

            fetch_odds >> validate_data
