"""
Business purpose: Build a portable curated event-outcome snapshot from a JSON results feed.
Source: JSON event outcomes at data/landing/event_outcomes/outcome_feed.json.
Target: Partitioned Parquet snapshot at data/curated/event_outcomes/published_outcomes.
Output grain: One row per outcome_id from the source feed.
Important rules: Standardize result attributes, add ingestion time, exclude the status code, and overwrite the target partitioned by event_id.
Generation profile: DATAFRAME_CHAIN + RESULT_SNAPSHOT_FLOW
Runtime arguments: None.
Assumptions: The mocked source feed exposes the declared fields and overwrite partitioning by event_id remains intended.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_event_outcome_pipeline").getOrCreate()

source_path = "data/landing/event_outcomes/outcome_feed.json"
target_path = "data/curated/event_outcomes/published_outcomes"

event_outcome_schema = StructType([
    StructField("outcome_id", IntegerType(), False),
    StructField("event_id", IntegerType(), True),
    StructField("participant_id", IntegerType(), True),
    StructField("organization_id", IntegerType(), True),
    StructField("vehicle_number", IntegerType(), True),
    StructField("starting_grid_position", IntegerType(), True),
    StructField("finish_position", IntegerType(), True),
    StructField("finish_position_text", StringType(), True),
    StructField("classification_order", IntegerType(), True),
    StructField("championship_points", FloatType(), True),
    StructField("completed_laps", IntegerType(), True),
    StructField("elapsed_time_text", StringType(), True),
    StructField("elapsed_milliseconds", IntegerType(), True),
    StructField("fastest_lap_number", IntegerType(), True),
    StructField("fastest_lap_rank", IntegerType(), True),
    StructField("fastest_lap_time_text", StringType(), True),
    StructField("fastest_lap_speed", FloatType(), True),
    StructField("status_code", StringType(), True),
])

# 2. Extract
event_outcome_raw_df = (
    spark.read
    .schema(event_outcome_schema)
    .json(source_path)
)

# 3. Standardize and add ingestion metadata
event_outcome_standardized_df = event_outcome_raw_df.select(
    col("outcome_id"),
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("vehicle_number"),
    col("starting_grid_position"),
    col("finish_position"),
    col("finish_position_text"),
    col("classification_order"),
    col("championship_points"),
    col("completed_laps"),
    col("elapsed_time_text"),
    col("elapsed_milliseconds"),
    col("fastest_lap_number"),
    col("fastest_lap_rank"),
    col("fastest_lap_time_text"),
    col("fastest_lap_speed"),
    col("status_code"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Build curated projection
published_event_outcomes_df = event_outcome_standardized_df.select(
    col("outcome_id"),
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("vehicle_number"),
    col("starting_grid_position"),
    col("finish_position"),
    col("finish_position_text"),
    col("classification_order"),
    col("championship_points"),
    col("completed_laps"),
    col("elapsed_time_text"),
    col("elapsed_milliseconds"),
    col("fastest_lap_number"),
    col("fastest_lap_rank"),
    col("fastest_lap_time_text"),
    col("fastest_lap_speed"),
    col("ingested_at_utc"),
)

# 5. Load
published_event_outcomes_df.write.mode("overwrite").partitionBy("event_id").parquet(target_path)

# 6. Release resources
spark.stop()
