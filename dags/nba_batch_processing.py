from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from betflow.historical.config import ProcessingConfig
import boto3
from pathlib import Path
from dotenv import load_dotenv
from betflow.historical.zach_glue_job_utils import create_glue_job
import os

load_dotenv()

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "start_date": ProcessingConfig.SPORT_CONFIGS["nba"]["start_date"],
    "end_date": ProcessingConfig.SPORT_CONFIGS["nba"]["end_date"],
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ProcessingConfig.TAGS["nba_games"],
}


def upload_glue_script():
    """Upload the Glue script to S3 before job execution"""
    s3_client = boto3.client("s3")
    # script_path = os.path.join(
    #     os.path.dirname(__file__), "glue_scripts/nba_games_processing.py"
    # )
    curr_dir = Path().resolve()
    print("curr directory: ", curr_dir)
    script_path = "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nba_glue_job.py"
    print(script_path)
    script_location = f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/glue_scripts/nba_glue_job.py"
    try:
        with open(script_path, "rb") as file:
            s3_client.put_object(
                Bucket=ProcessingConfig.S3_PATHS["processing_bucket"],
                Key="glue_scripts/nba_glue_job.py",
                Body=file,
            )
        return script_location
    except Exception as e:
        raise Exception(f"Failed to upload Glue script: {str(e)}")


with DAG(
    "nba_batch_processing",
    default_args=default_args,
    description="Process NBA games data into Iceberg tables",
    schedule_interval="@daily",
    catchup=True,
) as dag:
    # upload_script = PythonOperator(
    #     task_id="upload_script",
    #     python_callable=upload_glue_script,
    #     provide_context=True,
    # )

    run_job = PythonOperator(
        task_id="run_glue_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_pyspark_example_job",
            "script_path": "~/aeb_/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nba_glue_job.py",
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            # "tabular_credential": tabular_credential,
            "s3_bucket": "Zachwilsonorganization-522 bucket",
            "catalog_name": "glue_catalog",
            "aws_region": os.getenv("AWS_DEFAULT_REGION"),
            "description": "Testing Job Spark",
            "arguments": {
                "--JOB_NAME": "nba_games_processing",
                "--date": "{{ ds }}",
                "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/nba/",
                "--database_name": ProcessingConfig.GLUE_DB["db_name"],
                "--table_name": ProcessingConfig.GLUE_DB["nba_games_table"],
                "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
            },
        },
    )
