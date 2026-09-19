"""
Business purpose: publish each cargo drone flight with how long it took and how
fast it flew, so fleet utilisation reporting reads the measures instead of working
them out from the timestamps.
Source: data/landing/aerial/cargo_drone_flights, the Parquet flight log.
Target: the table aerial_analytics.cargo_drone_flight in the reporting database,
replaced on every run.
Output grain: one row per flight_id.
Business rules: a flight with any missing attribute is not published, the flight
duration is the minutes between launch and landing, and the average speed is the
route distance over that duration expressed per hour.
Generation profile: portable flat batch PySpark measure derivation.
Runtime arguments: argv[1] is the JDBC url of the reporting database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: the file path and the table name came from a configuration module in
the original, so they are written out here as the names above; credentials are
taken from the arguments rather than from that module; and the error handling of
the original reported a failure and re raised it, so it changed nothing about what
the pipeline produces.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("aerial_cargo_drone_flight_load").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/reporting"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the flight log
flight_landing_df = spark.read.parquet("data/landing/aerial/cargo_drone_flights").select(
    col("flight_id"),
    col("operator_id"),
    col("launch_time"),
    col("landing_time"),
    col("route_distance_km"),
    col("payload_kg"))

# 3. Apply business rules: every attribute must be present
flight_complete_df = flight_landing_df.na.drop()

# 4. Work out how long the flight took, in minutes
flight_timed_df = flight_complete_df.withColumn(
    "flight_duration_minutes",
    (unix_timestamp(col("landing_time")) - unix_timestamp(col("launch_time"))) / 60)

# 5. Work out how fast the flight was, per hour
cargo_drone_flight_df = flight_timed_df.withColumn(
    "average_speed_kmh",
    col("route_distance_km") / (col("flight_duration_minutes") / 60))

# 6. Replace the reporting table
cargo_drone_flight_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "aerial_analytics.cargo_drone_flight") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("overwrite") \
    .save()

# 7. Release resources
spark.stop()
