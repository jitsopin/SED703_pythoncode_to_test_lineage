"""Business purpose: Aggregate streaming taxi rides into hourly revenue and volume metrics per location.
Source: Kafka topic (rides on redpanda:29092)
Target: Operational PostgreSQL Database (JDBC) - public.processed_events_aggregated
Output grain: One record per 1-hour time window and pickup location ID.
Business rules: Extract JSON payload from Kafka, cast epoch milliseconds to standard timestamps, compute tumbling 1-hour windowed aggregates (trip counts and total revenue), and sink to PostgreSQL.
Generation profile: Portable PySpark script replacing PyFlink StreamExecutionEnvironment, Table API DDLs, and Flink watermarking logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, sum, to_timestamp, window
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaTaxiHourlyAggregationETL").getOrCreate()

# 2. Extract events from Kafka (Batch simulation of the stream)
kafka_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "rides") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transform: Parse JSON payload and derive event timestamps
taxi_schema = StructType([
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", LongType(), True)
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), taxi_schema).alias("data")
).select(
    col("data.PULocationID").alias("PULocationID"),
    col("data.DOLocationID").alias("DOLocationID"),
    col("data.trip_distance").alias("trip_distance"),
    col("data.total_amount").alias("total_amount"),
    to_timestamp(col("data.tpep_pickup_datetime") / 1000.0).alias("event_timestamp")
)

# 4. Transform: Apply 1-hour tumbling window aggregation
aggregated_df = parsed_df.groupBy(
    window(col("event_timestamp"), "1 hour"),
    col("PULocationID")
).agg(
    count("*").alias("num_trips"),
    sum("total_amount").alias("total_revenue")
).select(
    col("window.start").alias("window_start"),
    col("PULocationID"),
    col("num_trips"),
    col("total_revenue")
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