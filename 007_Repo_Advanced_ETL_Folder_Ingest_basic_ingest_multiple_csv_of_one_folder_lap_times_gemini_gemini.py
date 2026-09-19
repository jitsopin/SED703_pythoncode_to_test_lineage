"""Business purpose: Ingest, standardize, and load manufacturing cycle times from a folder of CSV files.
Source: data/landing/manufacturing/cycle_times (Folder of CSV files).
Target: data/curated/manufacturing/cycle_times_processed (Parquet format).
Output grain: One record per machine cycle.
Business rules: Read multiple CSV files, rename identifier columns, and append ingestion timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("ManufacturingCycleTimeIngestion").getOrCreate()

# 2. Define schema and read CSV directory
cycle_schema = StructType([
    StructField("plant_id", IntegerType(), False),
    StructField("station_id", IntegerType(), True),
    StructField("cycle_number", IntegerType(), True),
    StructField("quality_rating", IntegerType(), True),
    StructField("cycle_duration_str", StringType(), True),
    StructField("duration_ms", IntegerType(), True)
])

cycle_raw_df = spark.read.schema(cycle_schema).csv("data/landing/manufacturing/cycle_times")

# 3. Rename identifiers and append processing timestamp
cycle_standardized_df = cycle_raw_df.withColumnRenamed("plant_id", "factory_id") \
    .withColumnRenamed("station_id", "workstation_id") \
    .withColumn("processed_at", current_timestamp())

# 4. Write curated dataset to storage
cycle_standardized_df.write.mode("overwrite").parquet("data/curated/manufacturing/cycle_times_processed")

# 5. Release resources
spark.stop()