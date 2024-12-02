##############################
# Copied from Zach Wilson's repo and modified
# https://github.com/DataExpert-io/airflow-dbt-project
##############################

from botocore.exceptions import ClientError, NoCredentialsError
import time
import boto3
import json


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


def display_logs_from(paginator, run_id, log_group: str, continuation_token):
    """Mutualize iteration over the 2 different log streams glue jobs write to."""
    fetched_logs = []
    next_token = continuation_token
    try:
        for response in paginator.paginate(
            logGroupName=log_group,
            logStreamNames=[run_id],
            PaginationConfig={"StartingToken": continuation_token},
        ):
            fetched_logs.extend([event["message"] for event in response["events"]])
            # if the response is empty there is no nextToken in it
            next_token = response.get("nextToken") or next_token
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            # we land here when the log groups/streams don't exist yet
            print(
                "No new Glue driver logs so far.\n"
                "If this persists, check the CloudWatch dashboard at: %r.",
                "https://us-west-2.console.aws.amazon.com/cloudwatch/home",
            )
        else:
            raise

    if len(fetched_logs):
        # Add a tab to indent those logs and distinguish them from airflow logs.
        # Log lines returned already contain a newline character at the end.
        messages = "\t".join(fetched_logs)
        print("Glue Job Run %s Logs:\n\t%s", log_group, messages)
    else:
        print("No new log from the Glue Job in %s", log_group)
    return next_token


def check_job_status(glue_client, job_name, job_run_id):
    response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    status = response["JobRun"]
    return status


def create_glue_job(
    job_name,
    script_path,
    arguments,
    aws_access_key_id,
    aws_secret_access_key,
    s3_bucket,
    catalog_name,
    aws_region,
    description="Transform CSV data to Parquet format",
    kafka_credentials=None,
    polygon_credentials=None,
    max_concurrent_runs=5,
    **kwargs,
):
    script_path = upload_to_s3(script_path, s3_bucket, "jobscripts/" + script_path)

    if not script_path:
        raise ValueError("Uploading PySpark script to S3 failed!!")
    # we check to see if you passed in any --conf parameters to override the output table
    output_table = (
        kwargs["dag_run"].conf.get("output_table", arguments.get("--output_table", ""))
        if "dag_run" in kwargs
        else arguments.get("--output_table", "")
    )
    arguments["--output_table"] = output_table

    spark_configurations = [
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.defaultCatalog={catalog_name}",
        f"spark.sql.catalog.{catalog_name}=org.apache.iceberg.spark.SparkCatalog",
        # f"spark.sql.catalog.{catalog_name}.credential={tabular_credential}",
        f"spark.sql.defaultCatalog={catalog_name}",
        f"spark.sql.catalog.{catalog_name}=org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.catalog-impl=org.apache.iceberg.rest.RESTCatalog",
        f"spark.sql.catalog.{catalog_name}.warehouse={catalog_name}",
        f"spark.sql.catalog.{catalog_name}.uri=https://api.tabular.io/ws/",
        "spark.sql.shuffle.partitions=50",
    ]
    spark_string = " --conf ".join(spark_configurations)

    # Adding compatibility with Kafka and Iceberg here
    extra_jars_list = [
        f"s3://{s3_bucket}/jars/iceberg-spark-runtime-3.3_2.12-1.5.2.jar",
        f"s3://{s3_bucket}/jars/iceberg-aws-bundle-1.5.2.jar",
        f"s3://{s3_bucket}/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
        f"s3://{s3_bucket}/jars/hadoop-aws-3.2.0.jar",
    ]

    extra_jars = ",".join(extra_jars_list)

    python_modules_list = [
        # f"s3://{s3_bucket}/python-modules/websocket_client-1.8.0.tar.gz",
        "git+https://github.com/sanchitvj/sports_betting_analytics_engine.git",
    ]
    python_modules = ",".join(python_modules_list)
    job_args = {
        "Description": description,
        "Role": "AWSGlueServiceRole",
        "ExecutionProperty": {"MaxConcurrentRuns": int(max_concurrent_runs)},
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_path,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--conf": spark_string,
            "--extra-jars": extra_jars,
            "--additional-python-modules": python_modules,
        },
        "GlueVersion": "4.0",
        "WorkerType": "Standard",
        "NumberOfWorkers": 1,
    }

    logs_client = boto3.client(
        "logs",
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    glue_client = boto3.client(
        "glue",
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    error_continuation_token = None
    output_continuation_token = None
    try:
        # Try to get the existing job
        glue_client.get_job(JobName=job_name)
        print(f"Job '{job_name}' already exists. Updating it.")

        # Update the existing job
        response = glue_client.update_job(JobName=job_name, JobUpdate=job_args)
        print("Job update response:", json.dumps(response, indent=4))

    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Job '{job_name}' does not exist. Creating a new job.")
            response = glue_client.create_job(Name=job_name, **job_args)
            print(response)
        else:
            print(f"Unexpected error: {e}")

    if kafka_credentials is not None:
        arguments["--kafka_credentials"] = kafka_credentials
        arguments["--checkpoint_location"] = (
            f"""s3://{s3_bucket}/kafka-checkpoints/{job_name}"""
        )

    if polygon_credentials is not None:
        arguments["--polygon_credentials"] = polygon_credentials

    run_response = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
    log_group_default = "/aws-glue/jobs/output"
    log_group_error = "/aws-glue/jobs/error"
    job_run_id = run_response["JobRunId"]
    paginator = logs_client.get_paginator("filter_log_events")
    while True:
        status = check_job_status(glue_client, job_name, job_run_id)
        print(f"Job status: {status['JobRunState']}")
        if status["JobRunState"] in ["SUCCEEDED"]:
            break
        elif status["JobRunState"] in ["FAILED", "STOPPED"]:
            raise ValueError("Job has failed or stopped!")

        output_continuation_token = display_logs_from(
            paginator, job_run_id, log_group_default, output_continuation_token
        )
        error_continuation_token = display_logs_from(
            paginator, job_run_id, log_group_error, error_continuation_token
        )
        time.sleep(10)
    return job_name
