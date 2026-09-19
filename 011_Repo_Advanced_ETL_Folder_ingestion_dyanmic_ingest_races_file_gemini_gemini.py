"""Business purpose: Ingest, enrich, and partition aerospace launch schedules.
Source: data/landing/aerospace/launches.csv
Target: data/curated/aerospace/launches_partitioned
Output grain: One record per space launch mission.
Business rules: Combine date and time, inject data provider, select core fields, and partition by mission year.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("AerospaceLaunchIngestion").getOrCreate()
v_data_provider = "external_agency_api"

# 2. Extract raw space launch events
launch_schema = StructType([
    StructField("launch_id", IntegerType(), False),
    StructField("launch_year", IntegerType(), True),
    StructField("mission_phase", IntegerType(), True),
    StructField("site_id", IntegerType(), True),
    StructField("mission_name", StringType(), True),
    StructField("scheduled_date", DateType(), True),
    StructField("scheduled_time", StringType(), True),
    StructField("stream_url", StringType(), True)
])
launch_raw_df = spark.read.option("header", "true").schema(launch_schema).csv("data/landing/aerospace/launches.csv")

# 3. Derive scheduled timestamp and inject source provider
launch_enriched_df = launch_raw_df.withColumn("scheduled_timestamp", to_timestamp(concat(col("scheduled_date"), lit(" "), col("scheduled_time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_provider", lit(v_data_provider)) \
    .withColumn("processed_at", current_timestamp())

# 4. Select and rename columns for the curated layer
launch_selected_df = launch_enriched_df.select(
    col("launch_id").alias("flight_id"),
    col("launch_year").alias("mission_year"),
    col("mission_phase"),
    col("site_id").alias("launchpad_id"),
    col("mission_name"),
    col("data_provider"),
    col("processed_at"),
    col("scheduled_timestamp")
)

# 5. Load partitioned dataset to curated storage
launch_selected_df.write.mode("overwrite").partitionBy("mission_year").parquet("data/curated/aerospace/launches_partitioned")

# 6. Release resources
spark.stop()