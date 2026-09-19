"""
Business purpose: Build a portable curated competition-result snapshot from a JSON source.
Source: JSON competition results at data/landing/competition_results/result_events.json.
Target: Partitioned Parquet snapshot at data/curated/competition_results/published_results.
Output grain: One row per result_id from the source feed.
Important rules: Standardize result identifiers and timing fields, attach runtime data_source, add ingestion timestamp, exclude status_code, and overwrite partitioned by event_id.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_RESULT_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: The source JSON conforms to the declared schema and partitioned overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_competition_result_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/competition_results/result_events.json"
target_path = "data/curated/competition_results/published_results"

result_schema = StructType([
    StructField("result_id", IntegerType(), False),
    StructField("event_id", IntegerType(), True),
    StructField("participant_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("vehicle_number", IntegerType(), True),
    StructField("starting_grid", IntegerType(), True),
    StructField("finish_position", IntegerType(), True),
    StructField("finish_position_text", StringType(), True),
    StructField("classification_order", IntegerType(), True),
    StructField("points_awarded", FloatType(), True),
    StructField("laps_completed", IntegerType(), True),
    StructField("elapsed_time_text", StringType(), True),
    StructField("elapsed_milliseconds", IntegerType(), True),
    StructField("fastest_lap_number", IntegerType(), True),
    StructField("fastest_lap_rank", IntegerType(), True),
    StructField("fastest_lap_time_text", StringType(), True),
    StructField("fastest_lap_speed", FloatType(), True),
    StructField("status_code", StringType(), True),
])

# 2. Extract
result_raw_df = (
    spark.read
    .schema(result_schema)
    .json(source_path)
)

# 3. Attach runtime source metadata
result_sourced_df = result_raw_df.select(
    col("result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("vehicle_number"),
    col("starting_grid"),
    col("finish_position"),
    col("finish_position_text"),
    col("classification_order"),
    col("points_awarded"),
    col("laps_completed"),
    col("elapsed_time_text"),
    col("elapsed_milliseconds"),
    col("fastest_lap_number"),
    col("fastest_lap_rank"),
    col("fastest_lap_time_text"),
    col("fastest_lap_speed"),
    col("status_code"),
    lit(data_source).alias("data_source"),
)

# 4. Add ingestion metadata
result_ingested_df = result_sourced_df.select(
    col("result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("vehicle_number"),
    col("starting_grid"),
    col("finish_position"),
    col("finish_position_text"),
    col("classification_order"),
    col("points_awarded"),
    col("laps_completed"),
    col("elapsed_time_text"),
    col("elapsed_milliseconds"),
    col("fastest_lap_number"),
    col("fastest_lap_rank"),
    col("fastest_lap_time_text"),
    col("fastest_lap_speed"),
    col("status_code"),
    col("data_source"),
    current_timestamp().alias("ingested_at_utc"),
)

# 5. Build curated projection
published_result_snapshot_df = result_ingested_df.select(
    col("result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("vehicle_number"),
    col("starting_grid"),
    col("finish_position"),
    col("finish_position_text"),
    col("classification_order"),
    col("points_awarded"),
    col("laps_completed"),
    col("elapsed_time_text"),
    col("elapsed_milliseconds"),
    col("fastest_lap_number"),
    col("fastest_lap_rank"),
    col("fastest_lap_time_text"),
    col("fastest_lap_speed"),
    col("data_source"),
    col("ingested_at_utc"),
)

# 6. Load
published_result_snapshot_df.write.mode("overwrite").partitionBy("event_id").parquet(target_path)

# 7. Release resources
spark.stop()
