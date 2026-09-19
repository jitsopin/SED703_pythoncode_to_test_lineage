"""
Business purpose: count how many parcels are collected from each locker bank in
every minute, so servicing rounds and capacity planning read a continuously
updated count per bank and minute.
Source: the Kafka topic locker-pickup-events, whose messages carry one collection
as a JSON document with the locker bank and the moment it happened.
Target: warehouse/curated/locker/pickup_per_minute, written as Parquet and added
to as each minute closes.
Output grain: one row per locker_bank_id and one minute window.
Business rules: the moment of the collection arrives as epoch milliseconds and is
read as a timestamp, a collection that arrives more than one second late is not
counted, collections are grouped into fixed one minute windows, and both ends of
the window are published so a reader knows the period a count covers.
Generation profile: portable PySpark structured streaming windowed aggregation.
Runtime arguments: argv[1] is the Kafka bootstrap server list; it falls back to
"localhost:9092".
Assumptions: the source is a Flink job, so the declarations it wrote as connector
DDL are expressed here as reader and writer options, and the credentials it wrote
into that DDL are not carried over because the result is written to a dataset; the
late arrival allowance comes from the watermark its source table declared, since
the strategy object it also built was never attached to anything; its checkpoint
interval of ten seconds is expressed as the trigger interval; and both window
bounds are published, where it published only the start.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, get_json_object, window

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("locker_pickup_per_minute").getOrCreate()
bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"

# 2. Subscribe to the topic and read every message body as text
locker_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "locker-pickup-events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(col("value").cast("string").alias("json_str"))

# 3. Read the bank and the moment out of the body, and allow one second of lateness
locker_event_df = locker_raw_df.select(
    get_json_object(col("json_str"), "$.lockerBankId").cast("int").alias("locker_bank_id"),
    (get_json_object(col("json_str"), "$.eventEpochMillis").cast("long") / 1000)
    .cast("timestamp").alias("event_time")) \
    .withWatermark("event_time", "1 second")

# 4. Count the collections of every bank in each one minute window
locker_windowed_df = locker_event_df \
    .groupBy(window(col("event_time"), "1 minute"), col("locker_bank_id")) \
    .agg(count("*").alias("pickup_count"))

# 5. Publish both ends of the window beside the count
locker_pickup_df = locker_windowed_df.select(
    col("window.start").alias("event_minute_start"),
    col("window.end").alias("event_minute_end"),
    col("locker_bank_id"),
    col("pickup_count"))

# 6. Add every closed window to the curated dataset every ten seconds
locker_pickup_query = locker_pickup_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "warehouse/checkpoints/locker_pickup_per_minute") \
    .option("path", "warehouse/curated/locker/pickup_per_minute") \
    .trigger(processingTime="10 seconds") \
    .start()

# 7. Let the stream run, then close it down
locker_pickup_query.awaitTermination()

# 8. Release resources
spark.stop()
