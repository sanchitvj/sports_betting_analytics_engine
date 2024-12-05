from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from betflow.historical.config import ProcessingConfig

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


for sport, config in ProcessingConfig.SPORT_CONFIGS.items():
    with DAG(
        f"{sport}_odds_quality_check",
        default_args={
            **default_args,
            "start_date": config["start_date"],
            # "end_date": config["end_date"],
            "tags": ProcessingConfig.TAGS[f"{sport}_source_dqc"],
        },
        description=f"Validate {sport.upper()} odds data quality",
        schedule_interval="@daily",
        catchup=True,
    ) as dag:
        validate_source = PythonOperator(
            task_id=f"validate_{sport}_odds",
            python_callable=validate_json_structure,
            op_kwargs={"sport_key": sport},
            provide_context=True,
        )

    globals()[f"{sport}_quality_dag"] = dag
