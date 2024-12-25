from airflow import DAG
from airflow.utils.state import DagRunState
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from betflow.historical.config import ProcessingConfig


default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "snowflake_raw_to_staging",
    default_args=default_args,
    description="Refresh snowflake tables and run staging for games and odds",
    # schedule_interval="0 2 * * *",  # Run at 2 AM daily after sports_batch_processing
    schedule_interval=None,  # triggered by parent DAG
    start_date=datetime(2024, 12, 16),  # due to parent DAG
    catchup=True,
    max_active_runs=16,  # Increase concurrent DAG runs
    concurrency=32,  # Increase task concurrency
) as dag:
    wait_for_sports_batch = ExternalTaskSensor(
        task_id="wait_for_sports_batch",
        external_dag_id="sports_batch_processing",
        external_task_id=None,  # Wait for entire DAG
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        execution_date_fn=lambda dt: dt,
        mode="poke",
        timeout=7200,
        poke_interval=60,
        soft_fail=True,
        check_existence=True,
    )

    wait_for_odds_batch = ExternalTaskSensor(
        task_id="wait_for_odds_batch",
        external_dag_id="odds_batch_processing",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        execution_date_fn=lambda dt: dt,
        mode="poke",
        timeout=7200,
        poke_interval=60,
        soft_fail=True,
        check_existence=True,
    )

    # Refresh all external tables
    refresh_external_tables = SnowflakeOperator(
        task_id="refresh_external_tables",
        snowflake_conn_id="snowflake_conn",
        sql="""
        ALTER EXTERNAL TABLE sports_db.raw_layer.nba_games REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.nba_odds REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.nfl_games REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.nfl_odds REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.nhl_games REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.nhl_odds REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.cfb_games REFRESH;
        ALTER EXTERNAL TABLE sports_db.raw_layer.cfb_odds REFRESH;
        """,
    )

    # Run dbt models for games staging
    run_games_staging = BashOperator(
        task_id="run_games_staging",
        bash_command="cd /home/ubuntu/sports_betting_analytics_engine && dbt build --models staging.games.stg_nba_games staging.games.stg_nfl_games staging.games.stg_nhl_games staging.games.stg_cfb_games",
    )

    # Run dbt models for odds staging
    run_odds_staging = BashOperator(
        task_id="run_odds_staging",
        bash_command="cd /home/ubuntu/sports_betting_analytics_engine && dbt build --models staging.odds.stg_nba_odds staging.odds.stg_nfl_odds staging.odds.stg_nhl_odds staging.odds.stg_cfb_odds",
    )

    # Run dbt tests
    # run_dbt_tests = BashOperator(
    #     task_id="run_dbt_tests",
    #     bash_command="dbt test --models staging.*",
    # )

    (
        [wait_for_sports_batch, wait_for_odds_batch]
        >> refresh_external_tables
        >> [run_games_staging, run_odds_staging]
    )
