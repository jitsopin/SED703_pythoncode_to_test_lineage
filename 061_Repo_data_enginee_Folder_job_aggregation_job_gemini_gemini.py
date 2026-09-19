"""Business purpose: Aggregate streaming event logs into 1-minute interval metrics.
Source: Kafka topic (test-topic on redpanda-1:29092)
Target: Operational PostgreSQL Database (JDBC) - public.processed_events_aggregated
Output grain: One record per 1-minute time window and test_data key.
Business rules: Extract JSON event data from Kafka, cast raw milliseconds to proper timestamps, compute tumbling 1-minute windowed counts, and sink the aggregated hits to PostgreSQL.
Generation profile: Portable PySpark script replacing PyFlink StreamExecutionEnvironment, Table API DDLs, and WatermarkStrategy logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, to_timestamp, window
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaEventAggregationETL").getOrCreate()

# 2. Extract events from Kafka (Batch simulation of the stream)
kafka_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-1:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transform: Parse JSON payload and derive watermark timestamps
event_schema = StructType([
    StructField("test_data", IntegerType(), True),
    StructField("event_timestamp", LongType(), True)
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select(
    col("data.test_data").alias("test_data"),
    to_timestamp(col("data.event_timestamp") / 1000.0).alias("event_watermark")
)

# 4. Transform: Apply 1-minute tumbling window aggregation
aggregated_df = parsed_df.groupBy(
    window(col("event_watermark"), "1 minute"),
    col("test_data")
).agg(
    count("*").alias("num_hits")
).select(
    col("window.start").alias("event_hour"),
    col("test_data"),
    col("num_hits")
)

# 5. Load: Configure JDBC properties and sink to PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

aggregated_df.write.mode("append").jdbc(
    url=jdbc_url,
    table="public.processed_events_aggregated",
    properties=jdbc_properties
)

# 6. Release resources
spark.stop()