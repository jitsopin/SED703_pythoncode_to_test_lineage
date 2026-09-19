"""Business purpose: Process and standardize agricultural drone flight telemetry.
Source: data/landing/agriculture/drone_flights.json
Target: warehouse/agriculture/drone_metrics_partitioned
Output grain: One record per drone telemetry event.
Business rules: Apply defaults for missing drone types or zones, derive flight date, and partition.
Generation profile: Portable PySpark script replacing raw SQL inserts and AWS Glue metadata.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, date_format, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("AgriDroneTelemetryETL").getOrCreate()

# 2. Extract raw drone telemetry
drone_schema = StructType([
    StructField("flight_guid", StringType(), False),
    StructField("hardware_mac", StringType(), True),
    StructField("drone_type", StringType(), True),
    StructField("recorded_timestamp", TimestampType(), True),
    StructField("payload_json", StringType(), True),
    StructField("flight_speed_mps", DoubleType(), True),
    StructField("farm_zone", StringType(), True)
])
drone_raw_df = spark.read.schema(drone_schema).json("data/landing/agriculture/drone_flights.json")

# 3. Apply defaults simulating table constraints and derive partition keys
drone_enriched_df = drone_raw_df.withColumn("drone_type", coalesce(col("drone_type"), lit("UNKNOWN"))) \
    .withColumn("flight_speed_mps", coalesce(col("flight_speed_mps"), lit(0.0))) \
    .withColumn("farm_zone", coalesce(col("farm_zone"), lit("DEFAULT_ZONE"))) \
    .withColumn("flight_date", date_format(col("recorded_timestamp"), "yyyy-MM-dd"))

# 4. Standardize column names for final presentation
drone_final_df = drone_enriched_df.select(
    col("flight_guid").alias("event_id"),
    col("hardware_mac").alias("device_id"),
    col("drone_type"),
    col("recorded_timestamp").alias("event_time"),
    col("payload_json").alias("sensor_payload_json"),
    col("flight_speed_mps").alias("speed_mps"),
    col("farm_zone").alias("region"),
    col("flight_date")
)

# 5. Persist curated metrics partitioned by day and drone type
drone_final_df.write.mode("overwrite").partitionBy("flight_date", "drone_type").parquet("warehouse/agriculture/drone_metrics_partitioned")

# 6. Release Spark resources
spark.stop()