from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from betflow.historical.config import ProcessingConfig
from betflow.historical.zach_glue_job_utils import create_glue_job
from dotenv import load_dotenv
from airflow.models import Variable


load_dotenv()

default_args = {
    "owner": ProcessingConfig.OWNER,
    "depends_on_past": True,
    "start_date": ProcessingConfig.SPORT_CONFIGS["nba"][
        "start_date"
    ],  # datetime(2024, 10, 22),
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "tags": ProcessingConfig.TAGS["nba_games"],
}


# def process_games_data(**context):
#     """Process games data from raw S3 to Iceberg tables"""
#     date_str = context["ds"]
#
#     # os.environ["JAVA_HOME"] = (
#     #     "/usr/lib/jvm/java-11-openjdk-amd64"  # Adjust path as needed
#     # )
#     # os.environ["SPARK_HOME"] = "/usr/local/spark"  # Adjust path as needed
#
#     # Initialize Spark with AWS Glue catalog support
#     spark = (
#         SparkSession.builder.appName("nba_games_processing")
#         .config(
#             "spark.jars.packages",
#             "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4",
#         )
#         .config(
#             "spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#         )
#         .config(
#             "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
#         )
#         .config(
#             "spark.sql.catalog.glue_catalog.catalog-impl",
#             "org.apache.iceberg.aws.glue.GlueCatalog",
#         )
#         .config(
#             "spark.sql.catalog.glue_catalog.warehouse",
#             f"s3://{ProcessingConfig.S3_PATHS['processing_bucket']}/processed",
#         )
#         .config(
#             "spark.sql.catalog.glue_catalog.io-impl",
#             "org.apache.iceberg.aws.s3.S3FileIO",
#         )
#         .getOrCreate()
#     )
#
#     # Create database if not exists
#     spark.sql(
#         f"CREATE DATABASE IF NOT EXISTS glue_catalog.{ProcessingConfig.GLUE_DB['db_name']}"
#     )
#
#     # Create table with proper schema
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS glue_catalog.{ProcessingConfig.GLUE_DB['db_name']}.{ProcessingConfig.GLUE_DB['nba_games_table']} (
#             game_id STRING,
#             start_time TIMESTAMP,
#             status_state STRING,
#             status_detail STRING,
#             status_description STRING,
#             period INT,
#             clock STRING,
#             home_team STRUCT<
#                 id: STRING,
#                 name: STRING,
#                 abbreviation: STRING,
#                 score: STRING,
#                 field_goals: STRING,
#                 three_pointers: STRING,
#                 free_throws: STRING,
#                 rebounds: STRING,
#                 assists: STRING
#             >,
#             away_team STRUCT<
#                 id: STRING,
#                 name: STRING,
#                 abbreviation: STRING,
#                 score: STRING,
#                 field_goals: STRING,
#                 three_pointers: STRING,
#                 free_throws: STRING,
#                 rebounds: STRING,
#                 assists: STRING
#             >,
#             venue STRUCT<
#                 name: STRING,
#                 city: STRING,
#                 state: STRING
#             >,
#             broadcasts ARRAY<STRING>,
#             ingestion_timestamp TIMESTAMP
#         )
#         USING iceberg
#         PARTITIONED BY (year(start_time), month(start_time), day(start_time))
#     """)
#
#     # Read raw games data
#     raw_path = f"s3://{ProcessingConfig.S3_PATHS['raw_bucket']}/{ProcessingConfig.S3_PATHS['game_prefix']}/nba/{date_str}/games.json"
#     df = spark.read.json(raw_path)
#
#     # Handle multiple games in _1 array
#     games_df = df.select(explode("_1").alias("game"))
#
#     # Create temp view for transformation
#     games_df.createOrReplaceTempView("raw_games")
#
#     # Transform using Spark SQL
#     processed_df = spark.sql("""
#         SELECT
#             game_id,
#             CAST(start_time as timestamp) as start_time,
#             status_state,
#             status_detail,
#             status_description,
#             CAST(period as int) as period,
#             clock,
#             STRUCT(
#                 home_team_id as id,
#                 home_team_name as name,
#                 home_team_abbreviation as abbreviation,
#                 home_team_score as score,
#                 home_team_field_goals as field_goals,
#                 home_team_three_pointers as three_pointers,
#                 home_team_free_throws as free_throws,
#                 home_team_rebounds as rebounds,
#                 home_team_assists as assists
#             ) as home_team,
#             STRUCT(
#                 away_team_id as id,
#                 away_team_name as name,
#                 away_team_abbreviation as abbreviation,
#                 away_team_score as score,
#                 away_team_field_goals as field_goals,
#                 away_team_three_pointers as three_pointers,
#                 away_team_free_throws as free_throws,
#                 away_team_rebounds as rebounds,
#                 away_team_assists as assists
#             ) as away_team,
#             STRUCT(
#                 venue_name as name,
#                 venue_city as city,
#                 venue_state as state
#             ) as venue,
#             broadcasts,
#             from_unixtime(CAST(timestamp as long)) as ingestion_timestamp
#         FROM raw_games
#         WHERE status_state = 'post'
#     """)
#
#     # Write to Iceberg table with merge strategy
#     processed_df.writeTo(
#         f"glue_catalog.{ProcessingConfig.GLUE_DB['db_name']}.{ProcessingConfig.GLUE_DB['nba_games_table']}"
#     ).option("merge-schema", "true").append()
#
#     spark.stop()
#     return "Games processing completed"


with DAG(
    "nba_batch_processing",
    default_args=default_args,
    description="Process NBA games data into Iceberg tables and glue catalog",
    schedule_interval="@daily",
    catchup=True,
) as dag:
    # process_games = PythonOperator(
    #     task_id="process_games",
    #     python_callable=process_games_data,
    #     provide_context=True,
    # )
    run_job = PythonOperator(
        task_id="run_glue_job",
        python_callable=create_glue_job,
        op_kwargs={
            "job_name": "backfill_pyspark_example_job",
            "script_location": ProcessingConfig.SCRIPT_PATHS["nba_games"],
            # "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            # "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "s3_bucket": Variable.get("AWS_S3_BUCKET_TABULAR"),
            "catalog_name": Variable.get("CATALOG_NAME"),
            "aws_region": Variable.get("AWS_GLUE_REGION"),
            "description": "Testing batch processing glue job sports analytics",
            "arguments": {"--ds": "{{ ds }}"},
        },
    )
