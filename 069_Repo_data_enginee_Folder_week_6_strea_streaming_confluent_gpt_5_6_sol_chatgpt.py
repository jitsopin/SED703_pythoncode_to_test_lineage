"""
Business purpose: Continuously combine green-taxi and FHV ride message streams, parse the merged ride payloads using a supplied Spark schema, remove incomplete records, aggregate rides by PULocationID, sort by descending ride count, and publish a complete-state snapshot.
Sources: Streaming text files under data/landing/rides_confluent/green and data/landing/rides_confluent/fhv.
Target: JSON snapshot at data/curated/rides_confluent/pu_location_counts.
Output grain: One row per PULocationID containing its current ride count.
Important rules: Preserve independent green and FHV input branches; merge the streams before parsing; parse comma-space-delimited message values in runtime schema order; drop rows containing null parsed fields; group by PULocationID; count records; sort by count descending; refresh the complete aggregation snapshot every micro-batch.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (EXTERNALIZED_MULTI_STREAM_SCHEMA)
Runtime arguments: all_ride_schema_json_path as argv[1].
Assumptions: The original project-specific configuration and ALL_RIDE_SCHEMA are not included, so the exact schema cannot be reconstructed safely and is supplied at runtime as Spark StructType JSON. Provider authentication, broker/topic metadata, and Kafka relay behavior are represented portably by two file streams unioned before parsing; the original complete console sink is represented as an overwrite JSON snapshot via foreachBatch.
"""

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("portable_confluent_rides_aggregation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if len(sys.argv) < 2:
    raise ValueError("Required argument: all_ride_schema_json_path")

schema_path = sys.argv[1]
green_input_path = "data/landing/rides_confluent/green"
fhv_input_path = "data/landing/rides_confluent/fhv"
output_path = "data/curated/rides_confluent/pu_location_counts"
checkpoint_path = "data/curated/rides_confluent/checkpoints/pu_location_counts"

with open(schema_path, "r", encoding="utf-8") as schema_file:
    all_ride_schema = StructType.fromJson(json.load(schema_file))

schema_field_names = [field.name for field in all_ride_schema.fields]

if "PULocationID" not in schema_field_names:
    raise ValueError("all ride schema must contain PULocationID")

green_raw_df = (
    spark.readStream
    .format("text")
    .load(green_input_path)
)

fhv_raw_df = (
    spark.readStream
    .format("text")
    .load(fhv_input_path)
)

all_raw_df = green_raw_df.unionByName(
    fhv_raw_df
)

all_split_df = all_raw_df.withColumn(
    "_parts",
    split(col("value"), ", "),
)

all_parsed_df = all_split_df.select(
    *[
        col("_parts").getItem(index).cast(field.dataType).alias(field.name)
        for index, field in enumerate(all_ride_schema.fields)
    ]
)

all_clean_df = all_parsed_df.na.drop()

pu_location_count_df = all_clean_df.groupBy(
    col("PULocationID"),
).count()

pu_location_sorted_df = pu_location_count_df.orderBy(
    col("count").desc(),
)

snapshot_query = (
    pu_location_sorted_df.writeStream
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(
        lambda batch_df, batch_id: (
            batch_df.write
            .mode("overwrite")
            .json(output_path)
        )
    )
    .start()
)

snapshot_query.awaitTermination()

spark.stop()
