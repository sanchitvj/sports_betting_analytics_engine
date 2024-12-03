from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import timedelta
from betflow.historical.config import ProcessingConfig
import boto3
from dotenv import load_dotenv
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
    script_path = "/home/ubuntu/sports_betting_analytics_engine/src/betflow/historical/batch_processing/nba_glue_job.py"
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


def create_or_update_glue_job(**context):
    """Create or update the Glue job configuration"""
    glue_client = boto3.client("glue")
    script_location = context["task_instance"].xcom_pull(task_ids="upload_script")

    job_config = {
        "Name": "nba_games_processing",
        "Role": str(os.getenv("GLUE_ROLE")),
        "ExecutionProperty": {"MaxConcurrentRuns": 15},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--additional-python-modules": f"git+https://{os.getenv('GITHUB_TOKEN')}@github.com/sanchitvj/sports_betting_analytics_engine.git",
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
        glue_client.get_job(JobName="nba_games_processing")
        glue_client.update_job(JobName="nba_games_processing", JobUpdate=job_config)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_job(**job_config)


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

    setup_glue_job = PythonOperator(
        task_id="setup_glue_job",
        python_callable=create_or_update_glue_job,
        provide_context=True,
    )

    process_games = GlueJobOperator(
        task_id="process_games",
        job_name="nba_games_processing",
        s3_bucket=Variable.get("LOGS_BUCKET"),
        script_args={
            "--JOB_NAME": "nba_games_processing",
            "--date": "{{ ds }}",
            "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/nba/",
            "--database_name": ProcessingConfig.GLUE_DB["db_name"],
            "--table_name": ProcessingConfig.GLUE_DB["nba_games_table"],
            "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
        },
        region_name="us-east-1",
        verbose=True,
    )

    upload_script >> setup_glue_job >> process_games

    # process_games = GlueJobOperator(
    #     task_id="process_games",
    #     job_name="nba_games_processing",
    #     script_location="{{ task_instance.xcom_pull(task_ids='upload_script') }}",
    #     s3_bucket=Variable.get("LOGS_BUCKET"),
    #     iam_role_name=str(os.getenv("GLUE_ROLE")),
    #     create_job_kwargs={
    #         "GlueVersion": "4.0",
    #         "NumberOfWorkers": 2,
    #         "WorkerType": "G.1X",
    #         "ExecutionProperty": {"MaxConcurrentRuns": 15},
    #         "DefaultArguments": {
    #             # "--python-version": "3.11",
    #             "--additional-python-modules": f"git+https://{os.getenv('GITHUB_TOKEN')}@github.com/sanchitvj/sports_betting_analytics_engine.git",
    #             # "--extra-jars": f's3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
    #             "--enable-continuous-cloudwatch-log": "true",
    #             "--enable-glue-datacatalog": "true",
    #             "--enable-metrics": "true",
    #             "--conf": f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://{ProcessingConfig.S3_PATHS["processing_bucket"]}/processed',  # --conf spark.jars=s3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
    #             "--datalake-formats": "iceberg",
    #         },
    #         "MaxRetries": 0,  # Set maximum number of retries
    #         "Timeout": 300,  # Set timeout in minutes (e.g., 48 hours)
    #     },
    #     script_args={
    #         "--JOB_NAME": "nba_games_processing",
    #         "--date": "{{ ds }}",
    #         "--source_path": f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['games_prefix']}/nba/",
    #         "--database_name": ProcessingConfig.GLUE_DB["db_name"],
    #         "--table_name": ProcessingConfig.GLUE_DB["nba_games_table"],
    #         "--warehouse_path": f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
    #     },
    #     region_name="us-east-1",
    #     verbose=True,
    #     update_config=True,
    # )
    #
    # upload_script >> process_games
