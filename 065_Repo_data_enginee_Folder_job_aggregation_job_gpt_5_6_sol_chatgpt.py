"""
Business purpose: Continuously aggregate ride events into hourly pickup-zone metrics containing trip counts and total revenue.
Source: Streaming JSON files under data/landing/rides_hourly.
Target: Finalized hourly aggregates in Parquet at data/curated/rides_hourly/processed_events_aggregated.
Output grain: One row per finalized one-hour event-time window and PULocationID.
Important rules: Preserve PULocationID, DOLocationID, trip_distance, total_amount, and tpep_pickup_datetime; convert tpep_pickup_datetime from epoch milliseconds to event_timestamp; apply a five-second watermark; use one-hour tumbling windows; group by window start and PULocationID; calculate COUNT(*) as num_trips and SUM(total_amount) as total_revenue; emit finalized windows in append mode; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (HOURLY_EVENT_TIME_AGGREGATION)
Runtime arguments: None.
Assumptions: Kafka broker/topic configuration, Flink DDL, JDBC credentials, PostgreSQL primary-key/upsert behavior, and execution parallelism are environment-specific. A portable file-stream source plus append-only Parquet sink preserves the executed event-time aggregation semantics for finalized windows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, sum as spark_sum, window
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("portable_hourly_ride_aggregation").getOrCreate()

input_path = "data/landing/rides_hourly"
output_path = "data/curated/rides_hourly/processed_events_aggregated"
checkpoint_path = "data/curated/rides_hourly/checkpoints/processed_events_aggregated"

ride_schema = StructType([
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", LongType(), True),
])

rides_raw_df = (
    spark.readStream
    .schema(ride_schema)
    .json(input_path)
)

rides_timed_df = rides_raw_df.withColumn(
    "event_timestamp",
    expr("timestamp_millis(tpep_pickup_datetime)"),
)

rides_watermarked_df = rides_timed_df.withWatermark(
    "event_timestamp",
    "5 seconds",
)

rides_aggregated_df = (
    rides_watermarked_df
    .groupBy(
        window(col("event_timestamp"), "1 hour").alias("event_window"),
        col("PULocationID"),
    )
    .agg(
        count("*").alias("num_trips"),
        spark_sum("total_amount").alias("total_revenue"),
    )
)

rides_output_df = rides_aggregated_df.select(
    col("event_window.start").alias("window_start"),
    col("PULocationID"),
    col("num_trips"),
    col("total_revenue"),
)

stream_query = (
    rides_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

stream_query.awaitTermination()

spark.stop()
