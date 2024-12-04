from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta
import boto3
from betflow.historical.config import ProcessingConfig
from dotenv import load_dotenv
import os

load_dotenv()

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


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
            task_ids=f"upload_{sport}_script"
        )

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
                # "--additional-python-modules": "git+https://github.com/yourusername/betflow.git",
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


for sport, config in ProcessingConfig.SPORT_CONFIGS.items():
    with DAG(
        f"{sport}_batch_processing",
        default_args={
            **default_args,
            "start_date": config["start_date"],
            "tags": ProcessingConfig.TAGS[f"{sport}_games"],
        },
        description=f"Process {sport.upper()} games data into Iceberg tables",
        schedule_interval="@daily",
        catchup=True,
        # max_active_runs=3,
    ) as dag:
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
                "--date": "{{ macros.ds_add(ds, -1) }}",  # Previous day's data
                "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/{sport}/",
                "--database_name": ProcessingConfig.GLUE_DB["db_name"],
                "--table_name": ProcessingConfig.GLUE_DB[f"{sport}_games_table"],
                "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
            },
            region_name="us-east-1",
            trigger_rule="all_done",  # Continue even if upstream tasks fail
        )

        upload_script >> setup_glue_job >> process_games

    globals()[f"{sport}_dag"] = dag
