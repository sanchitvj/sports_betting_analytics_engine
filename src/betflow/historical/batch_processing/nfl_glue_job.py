import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "date",
        "source_path",
        "database_name",
        "table_name",
        "warehouse_path",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)

spark = (
    SparkSession.builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_catalog.warehouse", args["warehouse_path"])
    .getOrCreate()
)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{args['database_name']}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{args['database_name']}.{args['table_name']} (
        game_id STRING,
        start_time TIMESTAMP,
        partition_year INT,
        partition_month INT,
        partition_day INT,
        status_state STRING,
        status_detail STRING,
        status_description STRING,
        period INT,
        clock STRING,
        home_team STRUCT<
            id: STRING,
            name: STRING,
            abbreviation: STRING,
            score: INT,
            record: STRING,
            linescores: ARRAY<INT>
        >,
        away_team STRUCT<
            id: STRING,
            name: STRING,
            abbreviation: STRING,
            score: INT,
            record: STRING,
            linescores: ARRAY<INT>
        >,
        leaders STRUCT<
            passing: STRUCT<
                name: STRING,
                display_value: STRING,
                value: INT,
                team: STRING
            >,
            rushing: STRUCT<
                name: STRING,
                display_value: STRING,
                value: INT,
                team: STRING
            >,
            receiving: STRUCT<
                name: STRING,
                display_value: STRING,
                value: INT,
                team: STRING
            >
        >,
        venue STRUCT<
            name: STRING,
            city: STRING,
            state: STRING,
            indoor: BOOLEAN
        >,
        broadcasts ARRAY<STRING>,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (partition_year, partition_month, partition_day)
""")

raw_path = f"{args['source_path']}{args['date']}/games.json"
df = spark.read.json(raw_path)
if df.count() == 0:
    print(f"No games found for date: {args['date']}")
    job.commit()
    sys.exit(0)
df.createOrReplaceTempView("raw_games")

processed_df = spark.sql("""
    SELECT 
        game_id,
        TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'") as start_time,
        CAST(YEAR(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_year,
        CAST(MONTH(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_month,
        CAST(DAY(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_day,
        status_state,
        status_detail,
        status_description,
        CAST(period as int) as period,
        clock,
        STRUCT(
            home_team_id as id,
            home_team_name as name,
            home_team_abbreviation as abbreviation,
            CAST(home_team_score as INT) as score,
            home_team_record as record,
            home_team_linescores as linescores
        ) as home_team,
        STRUCT(
            away_team_id as id,
            away_team_name as name,
            away_team_abbreviation as abbreviation,
            CAST(away_team_score as INT) as score,
            away_team_record as record,
            away_team_linescores as linescores
        ) as away_team,
        STRUCT(
            STRUCT(
                passing_leader_name as name,
                passing_leader_display_value as display_value,
                CAST(passing_leader_value as INT) as value,
                passing_leader_team as team
            ) as passing,
            STRUCT(
                rushing_leader_name as name,
                rushing_leader_display_value as display_value,
                CAST(rushing_leader_value as INT) as value,
                rushing_leader_team as team
            ) as rushing,
            STRUCT(
                receiving_leader_name as name,
                receiving_leader_display_value as display_value,
                CAST(receiving_leader_value as INT) as value,
                receiving_leader_team as team
            ) as receiving
        ) as leaders,
        STRUCT(
            venue_name as name,
            venue_city as city,
            venue_state as state,
            venue_indoor as indoor
        ) as venue,
        broadcasts,
        CAST(TIMESTAMP_SECONDS(CAST(timestamp as LONG)) as TIMESTAMP) as ingestion_timestamp
    FROM raw_games
    WHERE status_state = 'post'
""")

null_check = processed_df.filter("""
        partition_year IS NULL OR
        partition_month IS NULL OR
        partition_day IS NULL OR
        home_team_score IS NULL OR
        away_team_score IS NULL OR
        home_team_name IS NULL OR
        away_team_name IS NULL OR
        start_time IS NULL
    """).count()
# print(f"Records with null partitions: {partition_check}")


# Check for duplicate game IDs
# duplicate_check = processed_df.groupBy("game_id").count().filter("count > 1")
# if duplicate_check.count() > 0:
#     print("Duplicate game IDs found:")
#     duplicate_check.show()

# Verify timestamp conversions
# print("Timestamp Distribution:")
# processed_df.groupBy("partition_year", "partition_month").count().show()

raw_count = df.count()
processed_count = processed_df.count()

processed_df.createOrReplaceTempView("processed_df")
reconciliation_check = spark.sql("""
        WITH raw_counts AS (
            SELECT DATE(start_time) as game_date,
                   COUNT(DISTINCT game_id) as raw_count
            FROM raw_games
            WHERE status_state = 'post'
            GROUP BY 1
        ),
        processed_counts AS (
            SELECT DATE(start_time) as game_date,
                   COUNT(DISTINCT game_id) as processed_count
            FROM processed_df
            GROUP BY 1
        )
        SELECT r.game_date, 
               r.raw_count, 
               p.processed_count,
               CASE WHEN r.raw_count != p.processed_count THEN 'Mismatch'
                    ELSE 'Match' END as status
        FROM raw_counts r
        LEFT JOIN processed_counts p ON r.game_date = p.game_date
        WHERE r.raw_count != p.processed_count
    """)

# print("\nGame-level Reconciliation:")
# reconciliation_check.show()

if (
    null_check == 0
    and raw_count == processed_count
    and reconciliation_check.count() == 0
):
    (
        processed_df.writeTo(
            f"glue_catalog.{args['database_name']}.{args['table_name']}"
        )
        .tableProperty("format-version", "2")
        .option("check-nullability", "false")
        .option("merge-schema", "true")
        .tableProperty("write.format.default", "parquet")
        .partitionedBy("partition_year", "partition_month", "partition_day")
        .append()
    )
else:
    error_msg = (
        "Data validation failed.\n Removing invalidated rows and keeping rest: \n"
    )
    validated_df = spark.sql("""
        WITH validation_check AS (
            SELECT 
                *,
                CASE 
                    WHEN home_team.score IS NULL OR away_team.score IS NULL THEN 'Invalid Score'
                    WHEN home_team.name IS NULL OR away_team.name IS NULL THEN 'Invalid Name'
                    WHEN partition_year IS NULL OR partition_month IS NULL OR 
                         partition_day IS NULL THEN 'Invalid Partition'
                    WHEN game_id IS NULL OR start_time IS NULL THEN 'Invalid Required Fields'
                    ELSE 'Valid'
                END as validation_status
            FROM processed_df
        )
        SELECT 
            game_id,
            start_time,
            partition_year,
            partition_month,
            partition_day,
            status_state,
            status_detail,
            status_description,
            period,
            clock,
            home_team,
            away_team,
            venue,
            broadcasts,
            ingestion_timestamp
        FROM validation_check
        WHERE validation_status = 'Valid'
        AND status_state = 'post'
    """)

    # print("\n=== Validation Summary ===")
    # print(f"Total records: {processed_df.count()}")
    # print(f"Valid records: {validated_df.count()}")
    # print(f"Filtered records: {processed_df.count() - validated_df.count()}")

    if validated_df.count() > 0:
        (
            validated_df.writeTo(
                f"glue_catalog.{args['database_name']}.{args['table_name']}"
            )
            .tableProperty("format-version", "2")
            .option("check-nullability", "false")
            .option("merge-schema", "true")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("partition_year", "partition_month", "partition_day")
            .append()
        )
    else:
        if null_check > 0:
            error_msg += (
                f"- {null_check} records with null partitions for {args['date']}\n"
            )

        raise ValueError(error_msg)

job.commit()
