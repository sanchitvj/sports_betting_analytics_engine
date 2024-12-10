import boto3
from betflow.historical.config import ProcessingConfig
from dotenv import load_dotenv, find_dotenv
import os
from typing import Dict


load_dotenv(find_dotenv("my.env"), override=True)


def check_source_data(args: Dict[str, str], **context):
    """Check if source data exists in S3"""
    s3_client = boto3.client("s3")
    date_str = (context["data_interval_start"]).strftime(
        "%Y-%m-%d"
    )  # - timedelta(days=1)

    try:
        s3_client.head_object(
            Bucket=ProcessingConfig.S3_PATHS["raw_bucket"],
            Key=f"historical/{args['type']}/{args['sport']}/{date_str}/{args['type']}.json",
        )
        print(f"Found {args['type']} data for {args['sport']} on {date_str}")
        return True
    except Exception as e:
        print(
            f"No {args['type']} data found for {args['sport']} on {date_str}: {str(e)}"
        )
        return False


def upload_glue_script(args: Dict[str, str]):
    """Upload the Glue script to S3"""

    s3_client = boto3.client("s3")
    py_file_name = (
        f"{args['sport']}_glue_job.py"
        if args["type"] == "games"
        else "odds_glue_job.py"
    )
    script_path = f"{ProcessingConfig.SCRIPT_PATHS['base_path']}/{py_file_name}"
    s3_key = f"glue_scripts/{py_file_name}"
    bucket_script_location = (
        f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/{s3_key}"
    )

    try:
        with open(script_path, "rb") as file:
            s3_client.put_object(
                Bucket=ProcessingConfig.S3_PATHS["processing_bucket"],
                Key=s3_key,
                Body=file,
            )
        return bucket_script_location
    except Exception as e:
        raise Exception(f"Failed to upload Glue script: {str(e)}")


def create_or_update_glue_job(args: Dict[str, str], **context):
    """Create or update the Glue job configuration"""

    glue_client = boto3.client("glue", region_name="us-east-1")
    task_id = (
        f"{args['sport']}_tasks.upload_{args['sport']}_script"
        if args["type"] == "games"
        else "common_tasks.upload_odds_script"
    )
    script_location = context["task_instance"].xcom_pull(
        task_ids=task_id, key="return_value"
    )

    if not script_location:
        raise ValueError(
            f"Script location not found for {args['sport']} {args['type']}"
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
            "--enable-continuous-cloudwatch-log": "false",
            "--enable-glue-datacatalog": "true",
            "--enable-metrics": "false",
            "--conf": f'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://{ProcessingConfig.S3_PATHS["processing_bucket"]}/processed',  # --conf spark.jars=s3://{Variable.get("MISC_BUCKET")}/iceberg-spark-runtime-3.3_2.12-1.6.1.jar,s3://{Variable.get("MISC_BUCKET")}/iceberg-aws-bundle-1.6.1.jar',
            "--datalake-formats": "iceberg",
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "Timeout": 300,
    }

    try:
        glue_client.get_job(JobName=f"{args['sport']}_{args['type']}_processing")
        glue_client.update_job(
            JobName=f"{args['sport']}_{args['type']}_processing",
            JobUpdate=job_config,
        )
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_job(
            Name=f"{args['sport']}_{args['type']}_processing", **job_config
        )
