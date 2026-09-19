"""Business purpose: Ingest streaming NYC Green Taxi event logs for downstream operational analytics.
Source: Kafka topic (green-data on redpanda-1:29092)
Target: Operational PostgreSQL Database (JDBC) - public.taxi_events
Output grain: One record per completed taxi trip event.
Business rules: Extract JSON payload from Kafka, enforce the strict 20-column data contract (preserving raw string datetimes as defined in the source), and append to the operational relational database.
Generation profile: Portable PySpark script replacing PyFlink StreamExecutionEnvironment, Table API DDLs, and custom watermark expressions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaGreenTaxiIngestionETL").getOrCreate()

# 2. Extract events from Kafka (Batch simulation of the stream, starting from earliest offsets)
kafka_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-1:29092") \
    .option("subscribe", "green-data") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transform: Parse JSON payload enforcing the target DDL schema
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", StringType(), True),
    StructField("lpep_dropoff_datetime", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# Deserializing the binary Kafka 'value' column into individual columns
parsed_taxi_df = kafka_df.select(
    from_json(col("value").cast("string"), taxi_schema).alias("data")
).select("data.*")

# 4. Load: Configure JDBC properties and sink to PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

parsed_taxi_df.write.mode("append").jdbc(
    url=jdbc_url,
    table="public.taxi_events",
    properties=jdbc_properties
)

# 5. Release resources
spark.stop()