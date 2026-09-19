"""
Business purpose: Clean New York taxi trip records, derive trip duration and average speed, and replace a relational analytics table with the transformed dataset.
Source: Parquet dataset at data/landing/taxi_trips/ny_taxi_trips.parquet.
Target: JDBC table analytics.ny_taxi_trip_metrics.
Output grain: One row per source taxi trip remaining after rows containing null values are removed.
Important rules: Drop rows containing nulls; trip_duration is the difference between dropoff and pickup timestamps in minutes; average_speed is trip_distance divided by trip duration in hours; replace the relational target on each run.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (TAXI_METRICS)
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: The source Parquet schema contains tpep_pickup_datetime, tpep_dropoff_datetime, and trip_distance; remaining source columns are preserved through schema inference.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("portable_taxi_metrics_pipeline").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/analytics"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "analytics_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

source_path = "data/landing/taxi_trips/ny_taxi_trips.parquet"
target_table = "analytics.ny_taxi_trip_metrics"

taxi_raw_df = spark.read.parquet(source_path)

taxi_clean_df = taxi_raw_df.dropna()

taxi_duration_df = taxi_clean_df.withColumn(
    "trip_duration",
    (
        col("tpep_dropoff_datetime").cast("long")
        - col("tpep_pickup_datetime").cast("long")
    )
    / 60.0,
)

taxi_metrics_df = taxi_duration_df.withColumn(
    "average_speed",
    col("trip_distance") / (col("trip_duration") / 60.0),
)

(
    taxi_metrics_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
