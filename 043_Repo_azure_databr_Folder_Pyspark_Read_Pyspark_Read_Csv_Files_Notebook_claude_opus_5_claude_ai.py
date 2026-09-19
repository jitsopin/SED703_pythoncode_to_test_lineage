"""
Business purpose: land the transit leg timing feed in the curated layer so journey
time reporting reads typed columns instead of raw text.
Source: data/landing/transit/trip_leg_times.csv, the leg timing export with a
header row, whose leg clock is written as minutes, seconds and milliseconds and
whose missing sequence positions arrive as a sentinel value.
Target: the table transit_curated.trip_leg_time, stored as Delta and replaced on
every run.
Output grain: one row per trip, vehicle and leg.
Business rules: the column types come from a declared contract rather than from
the sampled rows, the leg clock is parsed with the m:ss.SSS pattern the feed uses,
the sentinel value is read as missing, and every column is published under its
business name.
Generation profile: portable flat batch PySpark typed ingestion into a Delta
table.
Runtime arguments: none.
Assumptions: the target database already exists and the Delta format is available
to the session; the original demonstrated a dozen ways of reading the same file
and only its last read reached a write, so that read is the pipeline here; its
write went to a placeholder location, so a named target is used instead; and its
nullValues option was spelled in a way that Spark ignores, which is corrected to
nullValue so the sentinel is actually read as missing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (DoubleType, IntegerType, StructField, StructType,
                               TimestampType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("transit_trip_leg_time_ingestion").getOrCreate()

# 2. Read the leg timing export with an explicit schema contract
trip_leg_landing_schema = StructType(fields=[
    StructField("tripId", IntegerType(), True),
    StructField("vehicleId", IntegerType(), True),
    StructField("legIndex", IntegerType(), True),
    StructField("sequencePosition", DoubleType(), True),
    StructField("legClock", TimestampType(), True),
    StructField("legMilliseconds", IntegerType(), True),
])

trip_leg_landing_df = spark.read \
    .schema(trip_leg_landing_schema) \
    .option("header", True) \
    .option("timestampFormat", "m:ss.SSS") \
    .option("nullValue", "-1") \
    .csv("data/landing/transit/trip_leg_times.csv")

# 3. Publish every column under its business name
trip_leg_time_df = trip_leg_landing_df.select(
    col("tripId").alias("trip_id"),
    col("vehicleId").alias("vehicle_id"),
    col("legIndex").alias("leg_index"),
    col("sequencePosition").alias("sequence_position"),
    col("legClock").alias("leg_clock"),
    col("legMilliseconds").alias("leg_milliseconds"))

# 4. Replace the curated table
trip_leg_time_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("transit_curated.trip_leg_time")

# 5. Release resources
spark.stop()
