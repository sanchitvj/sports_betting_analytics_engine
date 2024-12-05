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
try:
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

    print(f"Processed rows: {processed_df.count()}")
    processed_df.show(5)

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
        print("processed df count is not greater than zero.")
    job.commit()

except Exception as e:
    print("GETTING EXCEPTION: ", e)
    if "no such file or directory" in str(e).lower():
        print(f"No data file found for date: {args['date']}")
        job.commit()
        sys.exit(0)
