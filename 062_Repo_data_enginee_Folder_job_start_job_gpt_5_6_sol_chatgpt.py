"""
Business purpose: Continuously process event records by converting epoch-millisecond timestamps to Spark timestamps and publishing the normalized stream.
Source: Streaming JSON files under data/landing/events_stream.
Target: Parquet dataset at data/curated/events_stream/processed_events.
Output grain: One row per input event.
Important rules: Preserve test_data and event_timestamp semantics; convert event_timestamp from epoch milliseconds to a timestamp column; apply a five-second event-time watermark; process newly arriving files only; append normalized records continuously; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (STREAM_NORMALIZATION)
Runtime arguments: None.
Assumptions: Kafka broker details, latest-offset configuration, Flink table DDL, JDBC credentials, and PostgreSQL connector metadata are environment-specific. A portable file-stream source naturally processes files arriving after query start and an append-only Parquet sink preserves the executed normalization behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("portable_event_stream_normalization").getOrCreate()

input_path = "data/landing/events_stream"
output_path = "data/curated/events_stream/processed_events"
checkpoint_path = "data/curated/events_stream/checkpoints/processed_events"

event_schema = StructType([
    StructField("test_data", IntegerType(), True),
    StructField("event_timestamp", LongType(), True),
])

events_raw_df = (
    spark.readStream
    .schema(event_schema)
    .json(input_path)
)

events_timed_df = events_raw_df.withColumn(
    "event_time",
    expr("timestamp_millis(event_timestamp)"),
)

events_watermarked_df = events_timed_df.withWatermark(
    "event_time",
    "5 seconds",
)

processed_events_df = events_watermarked_df.select(
    col("test_data"),
    col("event_time").alias("event_timestamp"),
)

stream_query = (
    processed_events_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

stream_query.awaitTermination()

spark.stop()
