from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta, datetime
from betflow.historical.config import ProcessingConfig
from betflow.historical.hist_utils import (
    check_source_data,
    upload_glue_script,
    create_or_update_glue_job,
)


default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,  # past dependency blocking from running so switched to False
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ["backup", "processing", "historical", "sports"],
}


with DAG(
    "backup_sports_batch_processing",
    default_args=default_args,
    start_date=datetime(2022, 7, 31),
    end_date=datetime(2024, 12, 19),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=16,
    concurrency=16,
) as dag:
    sports_to_process = ["nba", "nhl"]

    for sport in sports_to_process:
        with TaskGroup(group_id=f"{sport}_tasks") as sport_tasks:
            check_data = ShortCircuitOperator(
                task_id=f"check_{sport}_data",
                python_callable=check_source_data,
                op_kwargs={"args": {"type": "games", "sport": sport}},
                provide_context=True,
            )

            upload_script = PythonOperator(
                task_id=f"upload_{sport}_script",
                python_callable=upload_glue_script,
                op_kwargs={"args": {"type": "games", "sport": sport}},
                provide_context=True,
            )

            setup_glue_job = PythonOperator(
                task_id=f"setup_{sport}_glue_job",
                python_callable=create_or_update_glue_job,
                op_kwargs={"args": {"type": "games", "sport": sport}},
                provide_context=True,
            )

            process_games = GlueJobOperator(
                task_id=f"process_{sport}_games",
                job_name=f"{sport}_games_processing",
                s3_bucket=Variable.get("LOGS_BUCKET"),
                script_args={
                    "--JOB_NAME": f"{sport}_games_processing",
                    "--date": "{{ ds }}",  # Previous day's data: macros.ds_add(ds, -1)
                    "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/{sport}/",
                    "--database_name": ProcessingConfig.GLUE_DB["db_name"],
                    "--table_name": ProcessingConfig.GLUE_DB[f"{sport}_games_table"],
                    "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
                },
                region_name="us-east-1",
                trigger_rule="all_done",  # Continue even if upstream tasks fail
                concurrent_run_limit=10,
            )

            (check_data >> upload_script >> setup_glue_job >> process_games)
