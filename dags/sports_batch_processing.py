from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta, datetime
import boto3
from betflow.historical.config import ProcessingConfig
from dotenv import load_dotenv, find_dotenv
import os


load_dotenv(find_dotenv("my.env"), override=True)

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def check_source_data(sport: str, **context):
    """Check if source data exists in S3"""
    s3_client = boto3.client("s3")
    date_str = (context["data_interval_start"]).strftime(
        "%Y-%m-%d"
    )  # - timedelta(days=1)

    try:
        s3_client.head_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=f"{ProcessingConfig.S3_PATHS['games_prefix']}/{sport}/{date_str}/games.json",
        )
        print(f"Found source data for {sport} on {date_str}")
        return True
    except Exception as e:
        print(f"No source data found for {sport} on {date_str}: {str(e)}")
        return False


def upload_glue_script(sport: str):
    """Upload the Glue script to S3"""

    def _upload():
        s3_client = boto3.client("s3")
        script_path = ProcessingConfig.SCRIPT_PATHS[f"{sport}_games"]
        script_location = f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/glue_scripts/{sport}_glue_job.py"

        try:
            with open(script_path, "rb") as file:
                s3_client.put_object(
                    Bucket=ProcessingConfig.S3_PATHS["processing_bucket"],
                    Key=f"glue_scripts/{sport}_glue_job.py",
                    Body=file,
                )
            return script_location
        except Exception as e:
            raise Exception(f"Failed to upload Glue script: {str(e)}")

    return _upload


def create_or_update_glue_job(sport: str):
    """Create or update the Glue job configuration"""

    def _setup(**context):
        glue_client = boto3.client("glue", region_name="us-east-1")
        script_location = context["task_instance"].xcom_pull(
            task_ids=f"{sport}_tasks.upload_{sport}_script", key="return_value"
        )

        if not script_location:
            raise ValueError(f"Script location not found for {sport}")

        job_config = {
            # "Name": f"{sport}_games_processing",
            "Role": str(os.getenv("GLUE_ROLE")),
            "ExecutionProperty": {"MaxConcurrentRuns": 30},
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": script_location,
                "PythonVersion": "3",
            },
            "DefaultArguments": {
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--conf": f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://{ProcessingConfig.S3_PATHS["processing_bucket"]}/processed',  # --conf spark.jars=s3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
                "--datalake-formats": "iceberg",
            },
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 300,
        }

        try:
            glue_client.get_job(JobName=f"{sport}_games_processing")
            glue_client.update_job(
                JobName=f"{sport}_games_processing", JobUpdate=job_config
            )
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_job(Name=f"{sport}_games_processing", **job_config)

    return _setup


with DAG(
    "sports_batch_processing",
    default_args=default_args,
    description="Process all sports games data into Iceberg tables",
    schedule_interval="@daily",
    # schedule_interval="5 0 * * *",
    catchup=True,
    start_date=datetime(2024, 12, 2),
) as dag:
    for sport in ["nba"]:
        with TaskGroup(group_id=f"{sport}_tasks") as sport_tasks:
            check_data = ShortCircuitOperator(
                task_id=f"check_{sport}_data",
                python_callable=check_source_data,
                op_kwargs={"sport": sport},
                provide_context=True,
            )

            upload_script = PythonOperator(
                task_id=f"upload_{sport}_script",
                python_callable=upload_glue_script(sport),
                provide_context=True,
            )

            setup_glue_job = PythonOperator(
                task_id=f"setup_{sport}_glue_job",
                python_callable=create_or_update_glue_job(sport),
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
            )

            check_data >> upload_script >> setup_glue_job >> process_games

    # globals()[f"{sport}_dag"] = dag
