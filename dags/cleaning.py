from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_cleanup",
    default_args=default_args,
    description="Clean up bad data from Glue and S3",
    schedule_interval=timedelta(days=1),
)

cleanup_games = GlueJobOperator(
    task_id="cleanup_games_data",
    job_name="cleanup_sports_data",
    script_location="s3://your-bucket/scripts/cleanup_script.py",
    script_args={
        "--database": "SPORTS_DB",
        "--table": "games",
        "--s3_bucket": "cur-sp-data-aeb",
    },
    dag=dag,
)

cleanup_games
