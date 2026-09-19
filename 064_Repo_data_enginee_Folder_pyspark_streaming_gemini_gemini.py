"""Business purpose: Compute sliding window trip counts by taxi vendor from streaming CSV events.
Source: Kafka topic (rides_csv_topic on localhost:9092)
Target: Kafka topic (vendor_counts_topic on localhost:9092)
Output grain: One message per vendor per 10-minute window (sliding every 5 minutes).
Business rules: Extract binary Kafka messages, split the CSV payload to extract the vendor ID and pickup datetime, apply a sliding window aggregation, and format the output as key-value pairs for Kafka consumption.
Generation profile: Portable PySpark micro-batch script replacing the continuous Structured Streaming topology and dynamic array expansion.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, window, count, concat_ws

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaTaxiRideWindowingETL").getOrCreate()

# 2. Extract raw CSV messages from Kafka (Micro-batch simulation)
kafka_raw_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rides_csv_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transform: Parse CSV payload string and extract required fields
# Assuming index 0 is vendor_id and index 1 is tpep_pickup_datetime based on standard TLC data
parsed_rides_df = kafka_raw_df.select(
    split(col("value").cast("string"), ", ").getItem(0).cast("string").alias("vendor_id"),
    split(col("value").cast("string"), ", ").getItem(1).cast("timestamp").alias("tpep_pickup_datetime")
)

# 4. Transform: Apply sliding window aggregation (10 min duration, 5 min slide)
windowed_counts_df = parsed_rides_df.groupBy(
    window(col("tpep_pickup_datetime"), "10 minutes", "5 minutes"),
    col("vendor_id")
).agg(
    count("*").alias("trip_count")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("vendor_id"),
    col("trip_count")
)

# 5. Transform: Format dataset to Kafka Key-Value standard
kafka_payload_df = windowed_counts_df.select(
    col("vendor_id").cast("string").alias("key"),
    concat_ws(", ", col("window_start"), col("window_end"), col("trip_count")).alias("value")
)

# 6. Load: Publish windowed metrics back to Kafka
kafka_payload_df.write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "vendor_counts_topic") \
    .save()

# 7. Release resources
spark.stop()