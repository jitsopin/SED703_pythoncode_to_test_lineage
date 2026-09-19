"""Business purpose: Ingest and parse streaming taxi ride events for downstream operational staging.
Source: Kafka topic (rides on redpanda:29092)
Target: Operational PostgreSQL Database (JDBC) - public.processed_events
Output grain: One record per discrete taxi ride event.
Business rules: Extract JSON payload from Kafka starting from the latest offset, explicitly parse the schema, cast the raw epoch milliseconds to a human-readable timestamp, and append to the operational relational database.
Generation profile: Portable PySpark script replacing PyFlink StreamExecutionEnvironment, Table API DDLs, and Flink-specific timestamp conversion functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaTaxiPassthroughETL").getOrCreate()

# 2. Extract events from Kafka (Micro-batch simulation of the stream, starting from latest offsets)
kafka_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:29092") \
    .option("subscribe", "rides") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Transform: Parse JSON payload and convert epoch milliseconds to SQL timestamps
taxi_schema = StructType([
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", LongType(), True)
])

# Deserializing the binary Kafka 'value' column and transforming the timestamp
parsed_events_df = kafka_df.select(
    from_json(col("value").cast("string"), taxi_schema).alias("data")
).select(
    col("data.PULocationID").alias("PULocationID"),
    col("data.DOLocationID").alias("DOLocationID"),
    col("data.trip_distance").alias("trip_distance"),
    col("data.total_amount").alias("total_amount"),
    to_timestamp(col("data.tpep_pickup_datetime") / 1000.0).alias("pickup_datetime")
)

# 4. Load: Configure JDBC properties and sink to PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

parsed_events_df.write.mode("append").jdbc(
    url=jdbc_url,
    table="public.processed_events",
    properties=jdbc_properties
)

# 5. Release resources
spark.stop()