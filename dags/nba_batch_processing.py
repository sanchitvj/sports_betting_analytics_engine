from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta
from betflow.historical.config import ProcessingConfig
import boto3
from pathlib import Path
from dotenv import load_dotenv
import os
from airflow.models import Variable

load_dotenv()

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "start_date": ProcessingConfig.SPORT_CONFIGS["nba"]["start_date"],
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
    upload_script = PythonOperator(
        task_id="upload_script",
        python_callable=upload_glue_script,
        provide_context=True,
    )

    process_games = GlueJobOperator(
        task_id="process_games",
        job_name="nba_games_processing",
        script_location="{{ task_instance.xcom_pull(task_ids='upload_script') }}",
        s3_bucket=Variable.get("LOGS_BUCKET"),
        iam_role_name=str(os.getenv("GLUE_ROLE")),
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                # "--python-version": "3.11",
                "--additional-python-modules": f"git+https://{os.getenv('GITHUB_TOKEN')}@github.com/sanchitvj/sports_betting_analytics_engine.git",
                # "--extra-jars": f's3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--conf": f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://{ProcessingConfig.S3_PATHS["processing_bucket"]}/processed',  # --conf spark.jars=s3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
                "--datalake-formats": "iceberg",
            },
            "MaxRetries": 0,  # Set maximum number of retries
            "Timeout": 300,  # Set timeout in minutes (e.g., 48 hours)
        },
        script_args={
            "JOB_NAME": "nba_games_processing",
            "--date": "{{ ds }}",
            "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/nba/",
            "--database_name": ProcessingConfig.GLUE_DB["db_name"],
            "--table_name": ProcessingConfig.GLUE_DB["nba_games_table"],
            "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
        },
        region_name="us-east-1",
        verbose=True,
        update_config=True,
    )

    upload_script >> process_games
