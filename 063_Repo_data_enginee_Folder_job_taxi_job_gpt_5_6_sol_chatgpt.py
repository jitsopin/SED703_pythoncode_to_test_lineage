"""
Business purpose: Continuously ingest green-taxi event records, preserve the source business columns, derive an event-time pickup timestamp, apply lateness handling, and publish the normalized stream.
Source: Streaming JSON files under data/landing/taxi_events.
Target: Parquet dataset at data/curated/taxi_events.
Output grain: One row per input taxi event.
Important rules: Preserve the 20 source taxi columns; parse lpep_pickup_datetime using yyyy-MM-dd HH:mm:ss into pickup_timestamp; apply a 15-second watermark on pickup_timestamp; process all available source events from the beginning of the portable stream; append continuously; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (TAXI_EVENT_STREAM_INGEST)
Runtime arguments: None.
Assumptions: Kafka broker/topic settings, Flink table DDL, JDBC credentials, and PostgreSQL connector metadata are environment-specific. A portable file-stream source plus append-only Parquet sink preserves the executed streaming ingestion and event-time semantics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("portable_taxi_event_stream_ingest").getOrCreate()

input_path = "data/landing/taxi_events"
output_path = "data/curated/taxi_events"
checkpoint_path = "data/curated/taxi_events/checkpoints/main"

taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", StringType(), True),
    StructField("lpep_dropoff_datetime", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
])

taxi_raw_df = (
    spark.readStream
    .schema(taxi_schema)
    .json(input_path)
)

taxi_timed_df = taxi_raw_df.withColumn(
    "pickup_timestamp",
    to_timestamp(col("lpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
)

taxi_watermarked_df = taxi_timed_df.withWatermark(
    "pickup_timestamp",
    "15 seconds",
)

taxi_output_df = taxi_watermarked_df.select(
    col("VendorID"),
    col("lpep_pickup_datetime"),
    col("lpep_dropoff_datetime"),
    col("store_and_fwd_flag"),
    col("RatecodeID"),
    col("PULocationID"),
    col("DOLocationID"),
    col("passenger_count"),
    col("trip_distance"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("ehail_fee"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("payment_type"),
    col("trip_type"),
    col("congestion_surcharge"),
)

stream_query = (
    taxi_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

stream_query.awaitTermination()

spark.stop()
