"""
Business purpose: Continuously normalize ride events by converting epoch-millisecond pickup timestamps to Spark timestamps and publishing the processed stream.
Source: Streaming JSON files under data/landing/rides_pass_through.
Target: Parquet dataset at data/curated/rides_pass_through/processed_events.
Output grain: One row per input ride event.
Important rules: Preserve PULocationID, DOLocationID, trip_distance, and total_amount; convert tpep_pickup_datetime from epoch milliseconds to pickup_datetime; process newly arriving portable stream records; append continuously; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (STREAM_PASS_THROUGH_NORMALIZATION)
Runtime arguments: None.
Assumptions: Kafka broker/topic settings, Flink DDL, JDBC credentials, PostgreSQL connector metadata, and exact latest-offset semantics are environment-specific. A portable file-stream source plus append-only Parquet sink preserves the executed pass-through transformation and continuous-processing behavior for newly arriving files.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

spark = SparkSession.builder.appName("portable_ride_pass_through").getOrCreate()

input_path = "data/landing/rides_pass_through"
output_path = "data/curated/rides_pass_through/processed_events"
checkpoint_path = "data/curated/rides_pass_through/checkpoints/processed_events"

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
    "pickup_datetime",
    expr("timestamp_millis(tpep_pickup_datetime)"),
)

processed_events_df = rides_timed_df.select(
    col("PULocationID"),
    col("DOLocationID"),
    col("trip_distance"),
    col("total_amount"),
    col("pickup_datetime"),
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
