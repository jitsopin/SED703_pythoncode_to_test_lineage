"""Business purpose: Process and load IoT device events into a partitioned target.
Source: data/landing/iot/device_events.csv
Target: warehouse/iot/device_events_partitioned
Output grain: One record per device event.
Business rules: Append ingestion timestamp, rearrange schema to place partition column at the end, and perform a dynamic partition overwrite.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
Assumptions: The provided source was a utility module without a data flow. A generic IoT pipeline was generated to demonstrate the core transformations (timestamp addition, column rearrangement, partitioned write) as a flat script.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and dynamic partition configuration
spark = SparkSession.builder.appName("IoTDeviceEventProcessing").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 2. Extract raw device telemetry data
event_schema = StructType([
    StructField("event_date", StringType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("sensor_reading", StringType(), True),
    StructField("status_code", IntegerType(), True)
])
events_raw_df = spark.read.option("header", "true").schema(event_schema).csv("data/landing/iot/device_events.csv")

# 3. Append ingestion timestamp
events_timestamped_df = events_raw_df.withColumn("ingested_at", current_timestamp())

# 4. Rearrange columns to explicitly place the partition column at the end
column_order = [col for col in events_timestamped_df.schema.names if col != "event_date"] + ["event_date"]
events_reordered_df = events_timestamped_df.select(column_order)

# 5. Overwrite dynamic partitions in the curated table
events_reordered_df.write.mode("overwrite").partitionBy("event_date").parquet("warehouse/iot/device_events_partitioned")

# 6. Release resources
spark.stop()