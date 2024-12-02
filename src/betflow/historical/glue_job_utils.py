##############################
# Copied from Zach Wilson's repo and modified
# https://github.com/DataExpert-io/airflow-dbt-project
##############################

from botocore.exceptions import ClientError, NoCredentialsError
import time
import boto3


def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file} to {bucket}/{s3_file}")
        return f"s3://{bucket}/{s3_file}"
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Client error: {e}")
        return False


def check_job_status(glue_client, job_name, job_run_id):
    """Check the status of a Glue job run"""
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    return response["JobRun"]


def create_glue_job(
    job_name,
    script_location,
    aws_region,
    raw_bucket,
    processed_bucket,
    description="Process NBA games data to Iceberg format",
):
    """Create and run a Glue job for processing NBA games data"""

    # Configure Spark for Iceberg
    spark_configurations = [
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        f"spark.sql.catalog.glue_catalog.warehouse=s3://{processed_bucket}/processed",
        "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    ]
    spark_string = " --conf ".join(spark_configurations)

    # Add required JARs for Iceberg
    extra_jars_list = [
        "iceberg-spark-runtime-3.4_2.12:1.3.1",
        "iceberg-aws-bundle:1.3.1",
        "hadoop-aws:3.3.4",
    ]
    extra_jars = ",".join(extra_jars_list)

    # Add Python package from GitHub
    python_modules = "git+https://github.com/yourusername/betflow.git"

    job_args = {
        "Description": description,
        "Role": "AWSGlueServiceRole-NBA-Processing",
        "ExecutionProperty": {"MaxConcurrentRuns": 3},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--conf": spark_string,
            "--extra-jars": extra_jars,
            "--additional-python-modules": python_modules,
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            "--raw-bucket": raw_bucket,
            "--processed-bucket": processed_bucket,
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
    }

    glue_client = boto3.client("glue", region_name=aws_region)
    logs_client = boto3.client("logs", region_name=aws_region)

    try:
        glue_client.get_job(JobName=job_name)
        print(f"Job '{job_name}' exists. Updating configuration.")
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Creating new job '{job_name}'")
            response = glue_client.create_job(Name=job_name, **job_args)
        else:
            raise

    # Start the job run
    run_response = glue_client.start_job_run(
        JobName=job_name, Arguments={"--date": "{{ ds }}", "--sport": "nba"}
    )

    job_run_id = run_response["JobRunId"]

    # Monitor job status
    while True:
        status = check_job_status(glue_client, job_name, job_run_id)
        print(f"Job status: {status['JobRunState']}")

        if status["JobRunState"] == "SUCCEEDED":
            break
        elif status["JobRunState"] in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise ValueError(f"Job failed with status: {status['JobRunState']}")

        time.sleep(30)

    return job_name
