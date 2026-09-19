"""
Business purpose: Build a portable curated qualifying-session snapshot from a multiline JSON feed.
Source: Multiline JSON qualifying observations at data/landing/session_qualifying/qualifying_observations.json.
Target: Curated Parquet snapshot at data/curated/session_qualifying/published_qualifying_results.
Output grain: One row per qualifying_result_id from the source feed.
Important rules: Preserve Q1/Q2/Q3 session times, standardize identifiers, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + QUALIFYING_SESSION_FLOW
Runtime arguments: None.
Assumptions: The source JSON requires Spark multiline parsing and overwrite remains the intended load behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_qualifying_session_pipeline").getOrCreate()

source_path = "data/landing/session_qualifying/qualifying_observations.json"
target_path = "data/curated/session_qualifying/published_qualifying_results"

qualifying_result_schema = StructType([
    StructField("qualifying_result_id", IntegerType(), False),
    StructField("event_id", IntegerType(), True),
    StructField("participant_id", IntegerType(), True),
    StructField("organization_id", IntegerType(), True),
    StructField("vehicle_number", IntegerType(), True),
    StructField("qualifying_position", IntegerType(), True),
    StructField("q1_time_text", StringType(), True),
    StructField("q2_time_text", StringType(), True),
    StructField("q3_time_text", StringType(), True),
])

# 2. Extract
qualifying_result_raw_df = (
    spark.read
    .schema(qualifying_result_schema)
    .option("multiLine", True)
    .json(source_path)
)

# 3. Standardize and add ingestion metadata
qualifying_result_snapshot_df = qualifying_result_raw_df.select(
    col("qualifying_result_id"),
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("vehicle_number"),
    col("qualifying_position"),
    col("q1_time_text"),
    col("q2_time_text"),
    col("q3_time_text"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Build published projection
published_qualifying_results_df = qualifying_result_snapshot_df.select(
    col("qualifying_result_id"),
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("vehicle_number"),
    col("qualifying_position"),
    col("q1_time_text"),
    col("q2_time_text"),
    col("q3_time_text"),
    col("ingested_at_utc"),
)

# 5. Load
published_qualifying_results_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
