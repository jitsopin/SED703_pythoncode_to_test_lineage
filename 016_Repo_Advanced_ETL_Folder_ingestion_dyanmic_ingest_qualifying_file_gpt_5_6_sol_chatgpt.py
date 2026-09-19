"""
Business purpose: Build a portable curated qualifying-session snapshot from a multiline JSON source.
Source: Multiline JSON qualifying observations at data/landing/session_results/qualifying_activity.json.
Target: Curated Parquet snapshot at data/curated/session_results/published_qualifying_activity.
Output grain: One row per qualifying_result_id from the source feed.
Important rules: Preserve Q1/Q2/Q3 timing fields, standardize identifiers, attach runtime data_source, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_QUALIFYING_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: The source JSON requires Spark multiline parsing and overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_qualifying_activity_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/session_results/qualifying_activity.json"
target_path = "data/curated/session_results/published_qualifying_activity"

qualifying_schema = StructType([
    StructField("qualifying_result_id", IntegerType(), False),
    StructField("event_id", IntegerType(), True),
    StructField("participant_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("vehicle_number", IntegerType(), True),
    StructField("qualifying_position", IntegerType(), True),
    StructField("q1_time_text", StringType(), True),
    StructField("q2_time_text", StringType(), True),
    StructField("q3_time_text", StringType(), True),
])

# 2. Extract
qualifying_raw_df = (
    spark.read
    .schema(qualifying_schema)
    .option("multiLine", True)
    .json(source_path)
)

# 3. Add operational metadata
qualifying_enriched_df = qualifying_raw_df.select(
    col("qualifying_result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("vehicle_number"),
    col("qualifying_position"),
    col("q1_time_text"),
    col("q2_time_text"),
    col("q3_time_text"),
    current_timestamp().alias("ingested_at_utc"),
    lit(data_source).alias("data_source"),
)

# 4. Build published projection
published_qualifying_activity_df = qualifying_enriched_df.select(
    col("qualifying_result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("vehicle_number"),
    col("qualifying_position"),
    col("q1_time_text"),
    col("q2_time_text"),
    col("q3_time_text"),
    col("ingested_at_utc"),
    col("data_source"),
)

# 5. Load
published_qualifying_activity_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
