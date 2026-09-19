"""
Business purpose: Build a portable curated lap-performance snapshot from a folder of CSV files.
Source: CSV lap observations under data/landing/lap_performance/lap_observations/.
Target: Curated Parquet snapshot at data/curated/lap_performance/published_lap_observations.
Output grain: One row per event_id, participant_id, and lap_number from the source folder.
Important rules: Read all CSV files in the landing folder, standardize identifiers, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + LAP_FOLDER_FLOW
Runtime arguments: None.
Assumptions: Every CSV file in the landing folder conforms to the declared schema and overwrite remains intended.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_lap_performance_pipeline").getOrCreate()

source_path = "data/landing/lap_performance/lap_observations"
target_path = "data/curated/lap_performance/published_lap_observations"

lap_observation_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("participant_id", IntegerType(), True),
    StructField("lap_number", IntegerType(), True),
    StructField("running_position", IntegerType(), True),
    StructField("lap_time_text", StringType(), True),
    StructField("lap_milliseconds", IntegerType(), True),
])

# 2. Extract
lap_observation_raw_df = (
    spark.read
    .schema(lap_observation_schema)
    .csv(source_path)
)

# 3. Standardize and add ingestion metadata
lap_observation_snapshot_df = lap_observation_raw_df.select(
    col("event_id"),
    col("participant_id"),
    col("lap_number"),
    col("running_position"),
    col("lap_time_text"),
    col("lap_milliseconds"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Build published projection
published_lap_observations_df = lap_observation_snapshot_df.select(
    col("event_id"),
    col("participant_id"),
    col("lap_number"),
    col("running_position"),
    col("lap_time_text"),
    col("lap_milliseconds"),
    col("ingested_at_utc"),
)

# 5. Load
published_lap_observations_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
