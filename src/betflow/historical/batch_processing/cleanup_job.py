import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "database", "table", "s3_bucket"])

# Read Iceberg table
df = spark.read.format("iceberg").load(f"{args['database']}.{args['table']}")

# Apply data quality checks
df_cleaned = df.filter(
    (col("home_team_score").isNotNull())
    & (col("away_team_score").isNotNull())
    & (col("start_time").isNotNull())
    & (col("home_team_name").isNotNull())
    & (col("away_team_name").isNotNull())
)

# Identify bad records
bad_records = df.subtract(df_cleaned)

# Write cleaned data back to Iceberg table
df_cleaned.write.format("iceberg").mode("overwrite").saveAsTable(
    f"{args['database']}.{args['table']}"
)

# Delete bad records from S3
s3_path = f"s3://{args['s3_bucket']}/{args['database']}/{args['table']}/"
bad_files = (
    bad_records.select("_metadata.file_path")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

for file_path in bad_files:
    relative_path = file_path.replace(s3_path, "")
    s3.delete_object(
        Bucket=args["s3_bucket"],
        Key=f"{args['database']}/{args['table']}/{relative_path}",
    )

job.commit()
