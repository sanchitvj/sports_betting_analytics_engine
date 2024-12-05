from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from betflow.historical.config import HistoricalConfig
from betflow.historical.hist_utils import (
    validate_sports_json_structure,
    fetch_games_by_date,
    upload_to_s3_func,
)

default_args = {
    "owner": HistoricalConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["ingestion", "historical", "sports", "dqc"],
}

with DAG(
    "sports_ingestion_dqc",
    default_args=default_args,
    start_date=datetime(2024, 12, 4),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    for sport in ["nba", "nfl", "cfb", "nhl"]:
        with TaskGroup(f"{sport}_pipeline") as sport_group:
            # Fetch all seasons data at once
            fetch_games = PythonOperator(
                task_id=f"fetch_{sport}_games",
                python_callable=fetch_games_by_date,
                op_kwargs={"sport_key": sport},
            )

            # check_data = ShortCircuitOperator(
            #     task_id=f"check_{sport}_data",
            #     python_callable=check_games_data,
            #     op_kwargs={"sport_key": sport},
            # )

            # Data validation tasks
            validate_data = PythonOperator(
                task_id=f"validate_{sport}_json",
                python_callable=validate_sports_json_structure,
                op_kwargs={"sport_key": sport},
                trigger_rule="none_failed",  # Run only if check_data passes
            )

            # validate_games = PythonOperator(
            #     task_id=f"validate_{sport}_games",
            #     python_callable=validate_sports_game_data,
            #     op_kwargs={"sport": sport},
            # )
            #
            # validate_teams = PythonOperator(
            #     task_id=f"validate_{sport}_teams",
            #     python_callable=validate_team_data,
            #     op_kwargs={"sport": sport},
            # )
            #
            # validate_venues = PythonOperator(
            #     task_id=f"validate_{sport}_venues",
            #     python_callable=validate_venue_data,
            #     op_kwargs={"sport": sport},
            # )

            upload_to_s3 = PythonOperator(
                task_id=f"upload_{sport}_s3",
                python_callable=upload_to_s3_func,
                op_kwargs={"sport_key": sport, "kind": "games"},
            )

            # skip_sport = EmptyOperator(
            #     task_id=f"skip_{sport}_processing", trigger_rule="none_failed"
            # )

            fetch_games >> validate_data >> upload_to_s3
            # Set dependencies
            # fetch_games >> check_data
            # check_data >> [validate_json, skip_sport]
            # validate_json >> [validate_games, validate_teams, validate_venues]
            # [validate_games, validate_teams, validate_venues] >> upload_to_s3
