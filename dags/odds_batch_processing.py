from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import boto3
from betflow.historical.config import ProcessingConfig
from dotenv import load_dotenv, find_dotenv
import os
from airflow.models import Variable


load_dotenv(find_dotenv("my.env"), override=True)

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def upload_odds_glue_script():
    """Upload the common odds processing script to S3"""
    s3_client = boto3.client("s3")
    script_path = ProcessingConfig.SCRIPT_PATHS["odds_processing"]
    script_location = f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/glue_scripts/odds_glue_job.py"

    try:
        with open(script_path, "rb") as file:
            s3_client.put_object(
                Bucket=ProcessingConfig.S3_PATHS["processing_bucket"],
                Key="glue_scripts/odds_glue_job.py",
                Body=file,
            )
        return script_location
    except Exception as e:
        raise Exception(f"Failed to upload Glue script: {str(e)}")


def create_or_update_glue_job(sport: str):
    """Create or update the Glue job configuration"""

    def _setup(**context):
        glue_client = boto3.client("glue", region_name="us-east-1")
        script_location = context["task_instance"].xcom_pull(
            task_ids="common_tasks.upload_odds_script"
        )

        job_config = {
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
                "--conf": f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://{ProcessingConfig.S3_PATHS["processing_bucket"]}/processed',
                "--datalake-formats": "iceberg",
            },
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 300,
        }

        try:
            glue_client.get_job(JobName=f"{sport}_odds_processing")
            glue_client.update_job(
                JobName=f"{sport}_odds_processing", JobUpdate=job_config
            )
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_job(Name=f"{sport}_odds_processing", **job_config)

    return _setup


with DAG(
    "sports_odds_processing",
    default_args=default_args,
    description="Process all sports odds data into Iceberg tables",
    schedule_interval="@daily",
    catchup=True,
    start_date=min(
        config["start_date"] for config in ProcessingConfig.SPORT_CONFIGS.values()
    ),
) as dag:
    with TaskGroup("common_tasks") as common_tasks:
        upload_script = PythonOperator(
            task_id="upload_odds_script",
            python_callable=upload_odds_glue_script,
            provide_context=True,
        )

    for sport, config in ProcessingConfig.SPORT_CONFIGS.items():
        with TaskGroup(group_id=f"{sport}_tasks") as sport_tasks:
            setup_glue_job = PythonOperator(
                task_id=f"setup_{sport}_odds_job",
                python_callable=create_or_update_glue_job(sport),
                provide_context=True,
            )

            process_odds = GlueJobOperator(
                task_id=f"process_{sport}_odds",
                job_name=f"{sport}_odds_processing",
                s3_bucket=Variable.get("LOGS_BUCKET"),
                script_args={
                    "--JOB_NAME": f"{sport}_odds_processing",
                    "--date": "{{ macros.ds_add(ds, -1) }}",
                    "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['odds_prefix']}/{sport}/",
                    "--database_name": ProcessingConfig.GLUE_DB["db_name"],
                    "--table_name": ProcessingConfig.GLUE_DB[f"{sport}_odds_table"],
                    "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
                },
                region_name="us-east-1",
            )

            setup_glue_job >> process_odds

        common_tasks >> sport_tasks
