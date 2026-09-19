"""
Business purpose: Build a portable curated service-stop snapshot from a multiline JSON source.
Source: Multiline JSON service-stop events at data/landing/service_events/stop_activity.json.
Target: Curated Parquet snapshot at data/curated/service_events/published_stop_activity.
Output grain: One row per event_id, participant_id, and stop_sequence from the source feed.
Important rules: Preserve stop timing attributes, standardize identifiers, attach runtime data_source, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_MULTILINE_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: The source JSON requires Spark multiline parsing and overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_service_stop_activity_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/service_events/stop_activity.json"
target_path = "data/curated/service_events/published_stop_activity"

service_stop_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("participant_id", IntegerType(), True),
    StructField("stop_sequence", StringType(), True),
    StructField("lap_number", IntegerType(), True),
    StructField("stop_time_text", StringType(), True),
    StructField("duration_text", StringType(), True),
    StructField("duration_milliseconds", IntegerType(), True),
])

# 2. Extract
service_stop_raw_df = (
    spark.read
    .schema(service_stop_schema)
    .option("multiLine", True)
    .json(source_path)
)

# 3. Add operational metadata
service_stop_enriched_df = service_stop_raw_df.select(
    col("event_id"),
    col("participant_id"),
    col("stop_sequence"),
    col("lap_number"),
    col("stop_time_text"),
    col("duration_text"),
    col("duration_milliseconds"),
    current_timestamp().alias("ingested_at_utc"),
    lit(data_source).alias("data_source"),
)

# 4. Build published projection
published_service_stop_df = service_stop_enriched_df.select(
    col("event_id"),
    col("participant_id"),
    col("stop_sequence"),
    col("lap_number"),
    col("stop_time_text"),
    col("duration_text"),
    col("duration_milliseconds"),
    col("ingested_at_utc"),
    col("data_source"),
)

# 5. Load
published_service_stop_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
