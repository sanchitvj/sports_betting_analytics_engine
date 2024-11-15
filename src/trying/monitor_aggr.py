# monitor_aggregations.py
from pyspark.sql import SparkSession


def monitor_aggregations():
    spark = SparkSession.builder.appName("aggregation_monitor").getOrCreate()

    # Read from the checkpoint location
    df = spark.read.format("delta").load("/tmp/checkpoint/commits")

    # Show the aggregations
    df.show(truncate=False)

    # Or query specific metrics
    df.select(
        "window", "avg_temperature", "wind_impact_score", "weather_severity"
    ).show()


if __name__ == "__main__":
    monitor_aggregations()
