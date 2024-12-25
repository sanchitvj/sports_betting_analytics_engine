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
}

with DAG(
    "sports_betting_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 9),
    schedule_interval="@daily",
    catchup=True,
) as parent_dag:
    # Trigger sports ingestion DAG
    trigger_sports_ingestion = TriggerDagRunOperator(
        task_id="trigger_sports_ingestion",
        trigger_dag_id="sports_ingestion_dqc",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
    )

    # Trigger odds ingestion DAG
    trigger_odds_ingestion = TriggerDagRunOperator(
        task_id="trigger_odds_ingestion",
        trigger_dag_id="odds_ingestion_dqc",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
    )

    # Trigger transformation DAGs
    trigger_sports_batch = TriggerDagRunOperator(
        task_id="trigger_sports_batch",
        trigger_dag_id="sports_batch_processing",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
    )

    trigger_odds_batch = TriggerDagRunOperator(
        task_id="trigger_odds_batch",
        trigger_dag_id="odds_batch_processing",
        wait_for_completion=True,
        poke_interval=60,
        execution_date="{{ ds }}",
    )

    # # New weather triggers, for future
    # trigger_weather_ingestion = TriggerDagRunOperator(
    #     task_id="trigger_weather_ingestion",
    #     trigger_dag_id="weather_ingestion_dqc",
    #     wait_for_completion=True,
    #     poke_interval=60,
    #     trigger_rule="all_done"  # Run regardless of other tasks' status
    # )
    #
    # trigger_weather_batch = TriggerDagRunOperator(
    #     task_id="trigger_weather_batch",
    #     trigger_dag_id="weather_batch_processing",
    #     wait_for_completion=True,
    #     poke_interval=60,
    #     trigger_rule="all_done"
    # )

    trigger_sports_ingestion >> [trigger_odds_ingestion, trigger_sports_batch]
    trigger_odds_ingestion >> trigger_odds_batch

    # # Independent weather pipeline
    # trigger_weather_ingestion >> trigger_weather_batch
