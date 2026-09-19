"""
Business purpose: Build a portable curated service-stop snapshot from a multiline JSON feed.
Source: Multiline JSON stop events at data/landing/service_stops/stop_events.json.
Target: Curated Parquet snapshot at data/curated/service_stops/published_stop_events.
Output grain: One row per event_id, participant_id, and stop_sequence from the source feed.
Important rules: Preserve stop timing attributes, standardize identifiers, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + MULTILINE_EVENT_FLOW
Runtime arguments: None.
Assumptions: The source JSON requires Spark multiline parsing and overwrite remains the intended load behavior.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_service_stop_pipeline").getOrCreate()

source_path = "data/landing/service_stops/stop_events.json"
target_path = "data/curated/service_stops/published_stop_events"

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

# 3. Standardize and add ingestion metadata
service_stop_snapshot_df = service_stop_raw_df.select(
    col("event_id"),
    col("participant_id"),
    col("stop_sequence"),
    col("lap_number"),
    col("stop_time_text"),
    col("duration_text"),
    col("duration_milliseconds"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Build published projection
published_service_stops_df = service_stop_snapshot_df.select(
    col("event_id"),
    col("participant_id"),
    col("stop_sequence"),
    col("lap_number"),
    col("stop_time_text"),
    col("duration_text"),
    col("duration_milliseconds"),
    col("ingested_at_utc"),
)

# 5. Load
published_service_stops_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
