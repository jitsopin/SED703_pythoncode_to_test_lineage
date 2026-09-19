"""Business purpose: Ingest and parse raw event logs for downstream operational analysis.
Source: Kafka topic (test-topic on redpanda-1:29092)
Target: Operational PostgreSQL Database (JDBC) - public.processed_events
Output grain: One record per discrete event payload.
Business rules: Extract JSON event data from Kafka, cast raw milliseconds to human-readable timestamps, and load to the operational database.
Generation profile: Portable PySpark script replacing PyFlink StreamExecutionEnvironment, Table API DDLs, and Flink-specific timestamp functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("KafkaEventIngestionETL").getOrCreate()

# 2. Extract events from Kafka (Batch simulation of the stream, starting from latest offsets)
kafka_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-1:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Transform: Parse JSON payload and convert epoch milliseconds to SQL timestamps
event_schema = StructType([
    StructField("test_data", IntegerType(), True),
    StructField("event_timestamp", LongType(), True)
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select(
    col("data.test_data").alias("test_data"),
    to_timestamp(col("data.event_timestamp") / 1000.0).alias("event_timestamp")
)

# 4. Load: Configure JDBC properties and sink to PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

parsed_df.write.mode("append").jdbc(
    url=jdbc_url,
    table="public.processed_events",
    properties=jdbc_properties
)

# 5. Release resources
spark.stop()