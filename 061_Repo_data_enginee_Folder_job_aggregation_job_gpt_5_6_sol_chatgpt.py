"""
Business purpose: Continuously aggregate event hits by one-minute event-time windows and test_data values.
Source: Streaming JSON files under data/landing/events_stream.
Target: Finalized window aggregates in Parquet at data/curated/events_stream/processed_events_aggregated.
Output grain: One row per finalized one-minute event window and test_data value.
Important rules: Parse test_data and event_timestamp; convert epoch-millisecond event_timestamp to event_time; apply a one-second watermark; use one-minute tumbling windows; count events per window and test_data; emit finalized windows only; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (EVENT_TIME_STREAM_AGGREGATION)
Runtime arguments: None.
Assumptions: Flink/Kafka/JDBC connector metadata is environment-specific. A portable file-stream source and append-only Parquet sink preserve the executed event-time aggregation semantics. The unused five-second WatermarkStrategy object in the source is not applied by the executed Table API query; the one-second SQL watermark is preserved.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, window
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("portable_event_time_aggregation").getOrCreate()

input_path = "data/landing/events_stream"
output_path = "data/curated/events_stream/processed_events_aggregated"
checkpoint_path = "data/curated/events_stream/checkpoints/processed_events_aggregated"

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
    "1 second",
)

events_aggregated_df = (
    events_watermarked_df
    .groupBy(
        window(col("event_time"), "1 minute").alias("event_window"),
        col("test_data"),
    )
    .agg(count("*").alias("num_hits"))
)

events_output_df = events_aggregated_df.select(
    col("event_window.start").alias("event_hour"),
    col("test_data"),
    col("num_hits"),
)

stream_query = (
    events_output_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

stream_query.awaitTermination()

spark.stop()
