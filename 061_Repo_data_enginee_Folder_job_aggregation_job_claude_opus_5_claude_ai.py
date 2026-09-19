"""
Business purpose: count how many times each vending machine dispenses in every
minute, so service planning and stock reporting read a continuously updated count
per machine and minute.
Source: the Kafka topic vending-dispense-events, whose messages carry one dispense
event as a JSON document with the machine and the moment it happened.
Target: the table vending_curated.dispense_per_minute, appended as each minute
closes.
Output grain: one row per machine_id and event_minute.
Business rules: the moment of the event arrives as epoch milliseconds and is read
as a timestamp, an event that arrives more than one second late is not counted,
events are grouped into fixed one minute windows, and the window start is
published as the minute the count belongs to.
Generation profile: portable PySpark structured streaming windowed aggregation.
Runtime arguments: argv[1] is the Kafka bootstrap server list; it falls back to
"localhost:9092".
Assumptions: the target table already exists; the source is a Flink job, so the
declarations it wrote as connector DDL are expressed here as reader and writer
options, and the credentials it wrote into that DDL are not carried over because
the result is written through the catalog; the late arrival allowance comes from
the watermark its source table declared, since the strategy object it also built
was never attached to anything; and the result is appended when a window closes,
which is what its primary key on window and machine amounted to.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, window
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("vending_dispense_per_minute").getOrCreate()
bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"

# 2. Subscribe to the topic and read every message body as text
dispense_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "vending-dispense-events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(col("value").cast("string").alias("json_str"))

# 3. Read the machine and the moment out of the message body
dispense_event_schema = StructType(fields=[
    StructField("machineId", IntegerType(), True),
    StructField("eventEpochMillis", LongType(), True),
])

dispense_event_df = dispense_raw_df.select(
    from_json(col("json_str"), dispense_event_schema).alias("event")).select(
    col("event.machineId").alias("machine_id"),
    col("event.eventEpochMillis").alias("event_epoch_millis"))

# 4. Read the moment as a timestamp and allow one second of lateness
dispense_timed_df = dispense_event_df \
    .withColumn("event_time", (col("event_epoch_millis") / 1000).cast("timestamp")) \
    .withWatermark("event_time", "1 second")

# 5. Count the dispenses of every machine in each one minute window
dispense_windowed_df = dispense_timed_df \
    .groupBy(window(col("event_time"), "1 minute"), col("machine_id")) \
    .agg(count("*").alias("dispense_count"))

# 6. Publish the window start as the minute the count belongs to
dispense_per_minute_df = dispense_windowed_df.select(
    col("window.start").alias("event_minute"),
    col("machine_id"),
    col("dispense_count"))

# 7. Append every closed window to the curated table
dispense_query = dispense_per_minute_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "warehouse/checkpoints/dispense_per_minute") \
    .toTable("vending_curated.dispense_per_minute")

# 8. Let the stream run, then close it down
dispense_query.awaitTermination()

# 9. Release resources
spark.stop()
