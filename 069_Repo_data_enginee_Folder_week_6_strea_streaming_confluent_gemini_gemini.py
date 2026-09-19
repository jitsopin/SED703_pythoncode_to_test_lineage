"""Business purpose: Consolidate streaming TLC taxi logs (Green and FHV) to aggregate popular pickup locations.
Source 1: Kafka topic (green_taxi_topic on Confluent Cloud)
Source 2: Kafka topic (fhv_taxi_topic on Confluent Cloud)
Target: warehouse/taxis/popular_pickup_locations (Parquet format)
Output grain: One record per pickup location ID with an aggregated trip count.
Business rules: Extract binary Kafka messages from both fleet topics, union them, parse the CSV payload to extract the pickup location, filter invalid records, aggregate total trips per location, and persist the ranked results to the data lake.
Generation profile: Portable PySpark micro-batch script replacing redundant streaming hops, dynamic external schemas, and console sinks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaTaxiPickupAggregationETL").getOrCreate()

# Kafka Confluent Cloud Configuration
kafka_options = {
    "kafka.bootstrap.servers": "pkc-75m1o.europe-west3.gcp.confluent.cloud:9092",
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="API_KEY" password="API_SECRET";'
}

# 2. Extract and Union Green and FHV events from Kafka
green_raw_df = spark.read.format("kafka").options(**kafka_options).option("subscribe", "green_taxi_topic").load()
fhv_raw_df = spark.read.format("kafka").options(**kafka_options).option("subscribe", "fhv_taxi_topic").load()

all_raw_rides_df = green_raw_df.unionAll(fhv_raw_df)

# 3. Transform: Parse CSV payload string and extract PULocationID (Assuming index 0 is ride_id and index 1 is PULocationID)
parsed_rides_df = all_raw_rides_df.select(
    split(col("value").cast("string"), ", ").getItem(0).cast("string").alias("ride_id"),
    split(col("value").cast("string"), ", ").getItem(1).cast("integer").alias("PULocationID")
).na.drop()

# 4. Transform: Aggregate trip counts by pickup location and rank descending
pu_location_counts_df = parsed_rides_df.groupBy("PULocationID").agg(
    count("*").alias("trip_count")
).orderBy(col("trip_count").desc())

# 5. Load: Sink aggregated metrics to Data Lake (Replacing console sink)
pu_location_counts_df.write.mode("overwrite").parquet("warehouse/taxis/popular_pickup_locations")

# 6. Release resources
spark.stop()