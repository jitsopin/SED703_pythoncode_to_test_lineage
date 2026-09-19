"""
Business purpose: Continuously parse ride messages using a supplied Spark schema, compute vendor-level trip counts, compute 10-minute pickup-time windows sliding every 5 minutes, and publish the windowed vendor counts as a portable complete-state snapshot.
Source: Streaming text files under data/landing/rides_stream, where each line contains the original comma-space-delimited message value.
Target: JSON snapshot at data/curated/rides_stream/windowed_vendor_counts; console streams are retained for the raw parsed rides and vendor counts as diagnostic outputs.
Output grain: One row per 10-minute pickup-time window and vendor_id in the main persisted output.
Important rules: Parse message attributes in schema order using delimiter ", "; require vendor_id and tpep_pickup_datetime in the supplied schema; compute count by vendor_id; compute windowed count by 10-minute window with a 5-minute slide; format the main result as key=vendor_id and value=count; refresh the persisted snapshot from the complete streaming result on each micro-batch.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (EXTERNALIZED_STREAM_SCHEMA)
Runtime arguments: ride_schema_json_path as argv[1].
Assumptions: The imported project settings module is not included in the source, so the exact RIDE_SCHEMA cannot be reconstructed safely. The portable rewrite requires a Spark StructType JSON file at runtime and validates that vendor_id and tpep_pickup_datetime are present. Kafka broker/topic metadata is replaced by a portable text-file stream. The original Kafka complete-mode sink is represented as an overwrite JSON snapshot via foreachBatch, preserving complete-state output semantics without a Kafka service.
"""

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, split, window
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("portable_rides_streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if len(sys.argv) < 2:
    raise ValueError("Required argument: ride_schema_json_path")

ride_schema_json_path = sys.argv[1]
input_path = "data/landing/rides_stream"
output_path = "data/curated/rides_stream/windowed_vendor_counts"
output_checkpoint_path = "data/curated/rides_stream/checkpoints/windowed_vendor_counts"

with open(ride_schema_json_path, "r", encoding="utf-8") as schema_file:
    ride_schema = StructType.fromJson(json.load(schema_file))

schema_field_names = [field.name for field in ride_schema.fields]

if "vendor_id" not in schema_field_names or "tpep_pickup_datetime" not in schema_field_names:
    raise ValueError("ride schema must contain vendor_id and tpep_pickup_datetime")

rides_raw_df = (
    spark.readStream
    .format("text")
    .load(input_path)
)

rides_split_df = rides_raw_df.withColumn(
    "_parts",
    split(col("value"), ", "),
)

rides_parsed_df = rides_split_df.select(
    *[
        col("_parts").getItem(index).cast(field.dataType).alias(field.name)
        for index, field in enumerate(ride_schema.fields)
    ]
)

raw_console_query = (
    rides_parsed_df.writeStream
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .format("console")
    .option("truncate", False)
    .start()
)

vendor_count_df = rides_parsed_df.groupBy(
    col("vendor_id"),
).count()

vendor_console_query = (
    vendor_count_df.writeStream
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .format("console")
    .option("truncate", False)
    .start()
)

windowed_vendor_count_df = (
    rides_parsed_df
    .groupBy(
        window(
            col("tpep_pickup_datetime"),
            "10 minutes",
            "5 minutes",
        ).alias("pickup_window"),
        col("vendor_id"),
    )
    .count()
)

windowed_messages_df = windowed_vendor_count_df.select(
    col("vendor_id").cast("string").alias("key"),
    concat_ws(", ", col("count").cast("string")).alias("value"),
    col("pickup_window"),
)

snapshot_query = (
    windowed_messages_df.writeStream
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", output_checkpoint_path)
    .foreachBatch(
        lambda batch_df, batch_id: (
            batch_df.write
            .mode("overwrite")
            .json(output_path)
        )
    )
    .start()
)

spark.streams.awaitAnyTermination()

raw_console_query.stop()
vendor_console_query.stop()
snapshot_query.stop()

spark.stop()
