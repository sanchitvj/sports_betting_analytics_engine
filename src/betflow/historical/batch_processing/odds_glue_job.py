import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

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

# Create table with proper schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{args['database_name']}.{args['table_name']} (
        game_id STRING,
        sport_key STRING,
        sport_title STRING,
        commence_time TIMESTAMP,
        home_team STRING,
        away_team STRING,
        bookmaker_key STRING,
        bookmaker_title STRING,
        bookmaker_last_update TIMESTAMP,
        market_key STRING,
        market_last_update TIMESTAMP,
        home_price DOUBLE,
        away_price DOUBLE,
        partition_year INT,
        partition_month INT,
        partition_day INT,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (partition_year, partition_month, partition_day)
""")

# Read and process data
raw_path = f"{args['source_path']}{args['date']}/odds.json"
df = spark.read.json(raw_path)
if df.count() == 0:
    print(f"No odds data found for date: {args['date']}")
    job.commit()
    sys.exit(0)

# Explode the data array and bookmakers array
df_exploded = df.select(explode("data").alias("data"))
df_exploded.createOrReplaceTempView("raw_odds")

processed_df = spark.sql("""
    WITH exploded_bookmakers AS (
    SELECT 
        data.id as game_id,
        data.sport_key,
        data.sport_title,
        CAST(data.commence_time as timestamp) as commence_time,
        data.home_team,
        data.away_team,
        explode(data.bookmakers) as bookmaker,
        CAST(YEAR(TO_TIMESTAMP(data.commence_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")) as INT) as partition_year,
        CAST(MONTH(TO_TIMESTAMP(data.commence_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")) as INT) as partition_month,
        CAST(DAY(TO_TIMESTAMP(data.commence_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")) as INT) as partition_day,
        CAST(current_timestamp() as timestamp) as ingestion_timestamp
    FROM raw_odds
),
exploded_markets AS (
SELECT 
    *,
    explode(bookmaker.markets) as market
FROM exploded_bookmakers
)
SELECT 
    game_id,
    sport_key,
    sport_title,
    commence_time,
    home_team,
    away_team,
    bookmaker.key as bookmaker_key,
    bookmaker.title as bookmaker_title,
    CAST(bookmaker.last_update as timestamp) as bookmaker_last_update,
    market.key as market_key,
    CAST(market.last_update as timestamp) as market_last_update,
    CAST(market.outcomes[0].price as DOUBLE) as home_price,
    CAST(market.outcomes[1].price as DOUBLE) as away_price,
    partition_year,
    partition_month,
    partition_day,
    ingestion_timestamp
FROM exploded_markets
WHERE market.key = 'h2h'
""")

# print(f"Processed rows: {processed_df.count()}")
# processed_df.show(5)

null_partitions = processed_df.filter(
    "partition_year IS NULL OR partition_month IS NULL OR partition_day IS NULL"
).count()
print(f"Records with null partitions: {null_partitions}")

null_fields = processed_df.filter("""
        game_id IS NULL OR 
        sport_key IS NULL OR 
        commence_time IS NULL OR 
        home_team IS NULL OR 
        away_team IS NULL OR
        bookmaker_key IS NULL OR
        market_key IS NULL OR
        home_price IS NULL OR
        away_price IS NULL
    """).count()
print(f"Records with null required fields: {null_fields}")

processed_df.createOrReplaceTempView("processed_df")

# Corrected reconciliation check
reconciliation_check = spark.sql("""
    WITH raw_counts AS (
        SELECT DATE(data.commence_time) as game_date,
               COUNT(DISTINCT data.id) as raw_count
        FROM raw_odds
        GROUP BY 1
    ),
    processed_counts AS (
        SELECT DATE(commence_time) as game_date,
               COUNT(DISTINCT game_id) as processed_count
        FROM processed_df
        GROUP BY 1
    )
    SELECT 
        r.game_date, 
        r.raw_count, 
        p.processed_count,
        CASE WHEN r.raw_count != p.processed_count THEN 'Mismatch'
             ELSE 'Match' END as status
    FROM raw_counts r
    LEFT JOIN processed_counts p ON r.game_date = p.game_date
    WHERE r.raw_count != p.processed_count
""")

print("\nGame-level Reconciliation:")
reconciliation_check.show()

# 10000 standard American odds limit
invalid_prices = processed_df.filter(
    "home_price < -10001 OR home_price > 10001 OR away_price < -10001 OR away_price > 10001"
).count()
print(f"Records with invalid price ranges: {invalid_prices}")

# odds data will have future timestamps
# invalid_timestamps = processed_df.filter("""
#         commence_time > current_timestamp() OR
#         bookmaker_last_update > current_timestamp() OR
#         market_last_update > current_timestamp()
#     """).count()
# print(f"Records with future timestamps: {invalid_timestamps}")

if (
    null_partitions == 0 and null_fields == 0 and invalid_prices == 0
    # and invalid_timestamps == 0
):
    if processed_df.count() > 0:
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
        print(f"processed df count is not greater than zero for {args['date']}.")
else:
    error_msg = "Data validation failed:\n"
    if null_partitions > 0:
        error_msg += (
            f"- {null_partitions} records with null partitions for {args['date']}\n"
        )
    if null_fields > 0:
        error_msg += (
            f"- {null_fields} records with null required fields for {args['date']}\n"
        )
    if invalid_prices > 0:
        error_msg += (
            f"- {invalid_prices} records with invalid prices for {args['date']}\n"
        )
    # if invalid_timestamps > 0:
    #     error_msg += f"- {invalid_timestamps} records with future timestamps\n"
    raise ValueError(error_msg)

job.commit()
