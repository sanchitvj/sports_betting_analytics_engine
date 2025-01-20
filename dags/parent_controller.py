from datetime import datetime, timedelta
from airflow import DAG
from betflow.historical.config import ProcessingConfig
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "trigger_rule": "all_success",
}

with DAG(
    "sports_betting_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 9),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
) as parent_dag:
    # Trigger sports ingestion DAG
    trigger_sports_ingestion = TriggerDagRunOperator(
        task_id="trigger_sports_ingestion",
        trigger_dag_id="sports_ingestion_dqc",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
        reset_dag_run=True,  # Add this to reset existing DAG run
        allowed_states=["success"],  # Add this to consider successful runs
        trigger_rule="all_success",
    )

    # Trigger odds ingestion DAG
    trigger_odds_ingestion = TriggerDagRunOperator(
        task_id="trigger_odds_ingestion",
        trigger_dag_id="odds_ingestion_dqc",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
        reset_dag_run=True,  # Add this to reset existing DAG run
        allowed_states=["success"],  # Add this to consider successful runs
        trigger_rule="all_success",
    )

    # Trigger transformation DAGs
    trigger_sports_batch = TriggerDagRunOperator(
        task_id="trigger_sports_batch",
        trigger_dag_id="sports_batch_processing",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
        reset_dag_run=True,  # Add this to reset existing DAG run
        allowed_states=["success"],  # Add this to consider successful runs
        trigger_rule="all_success",
    )

    trigger_odds_batch = TriggerDagRunOperator(
        task_id="trigger_odds_batch",
        trigger_dag_id="odds_batch_processing",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
        reset_dag_run=True,  # Add this to reset existing DAG run
        allowed_states=["success"],  # Add this to consider successful runs
        trigger_rule="all_success",
    )

    trigger_snowflake_staging = TriggerDagRunOperator(
        task_id="trigger_games_staging",
        trigger_dag_id="snowflake_raw_to_staging",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
        reset_dag_run=True,  # Add this to reset existing DAG run
        allowed_states=["success"],  # Add this to consider successful runs
        trigger_rule="all_success",
    )

    # First layer - Ingestion
    trigger_sports_ingestion >> trigger_odds_ingestion

    # Second layer - Processing
    trigger_sports_ingestion >> trigger_sports_batch
    trigger_odds_ingestion >> trigger_odds_batch

    # Third layer - Staging
    [trigger_sports_batch, trigger_odds_batch] >> trigger_snowflake_staging
