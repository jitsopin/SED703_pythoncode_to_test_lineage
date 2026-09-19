"""
Business purpose: Build a portable presentation-ready competition result dataset by combining event, venue, participant, team, and result snapshots.
Sources: Curated Parquet datasets under data/curated/reference and data/curated/competition.
Target: Presentation Parquet dataset at data/presentation/competition/published_event_results.
Output grain: One row per participant result within an event.
Important rules: Inner-join events to venues, then join result facts to event context, participant, and team references; project reporting fields and add creation timestamp.
Generation profile: DATAFRAME_CHAIN + RACE_RESULTS_RELATIONAL_FLOW
Runtime arguments: None.
Assumptions: Join keys are unique enough in reference datasets to preserve one reporting row per participant result.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_event_result_presentation_pipeline").getOrCreate()

event_path = "data/curated/competition/event_calendar"
venue_path = "data/curated/reference/venue_directory"
participant_path = "data/curated/reference/participant_directory"
team_path = "data/curated/reference/team_directory"
result_path = "data/curated/competition/result_snapshot"
target_path = "data/presentation/competition/published_event_results"

# 2. Read event context
event_calendar_df = spark.read.parquet(event_path).select(
    col("event_id"),
    col("season_year"),
    col("event_name"),
    col("event_timestamp"),
    col("venue_id"),
)

# 3. Read venue reference
venue_directory_df = spark.read.parquet(venue_path).select(
    col("venue_id"),
    col("city").alias("venue_city"),
)

# 4. Join events to venues
event_venue_context_df = event_calendar_df.join(
    venue_directory_df,
    event_calendar_df.venue_id == venue_directory_df.venue_id,
    "inner",
).select(
    event_calendar_df.event_id,
    event_calendar_df.season_year,
    event_calendar_df.event_name,
    event_calendar_df.event_timestamp,
    venue_directory_df.venue_city,
)

# 5. Read participant reference
participant_directory_df = spark.read.parquet(participant_path).select(
    col("participant_id"),
    col("full_name").alias("participant_name"),
    col("vehicle_number"),
    col("nationality").alias("participant_nationality"),
)

# 6. Read team reference
team_directory_df = spark.read.parquet(team_path).select(
    col("team_id"),
    col("team_name"),
)

# 7. Read result facts
result_snapshot_df = spark.read.parquet(result_path).select(
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("starting_grid"),
    col("fastest_lap_number"),
    col("elapsed_time_text").alias("event_time_text"),
    col("points_awarded"),
    col("finish_position"),
)

# 8. Combine reporting entities
combined_event_result_df = (
    result_snapshot_df
    .join(
        event_venue_context_df,
        result_snapshot_df.event_id == event_venue_context_df.event_id,
        "inner",
    )
    .join(
        participant_directory_df,
        result_snapshot_df.participant_id == participant_directory_df.participant_id,
        "inner",
    )
    .join(
        team_directory_df,
        result_snapshot_df.team_id == team_directory_df.team_id,
        "inner",
    )
)

# 9. Build final presentation dataset
published_event_result_df = combined_event_result_df.select(
    col("season_year"),
    col("event_name"),
    col("event_timestamp"),
    col("venue_city"),
    col("participant_name"),
    col("vehicle_number"),
    col("participant_nationality"),
    col("team_name"),
    col("starting_grid"),
    col("fastest_lap_number"),
    col("event_time_text"),
    col("points_awarded"),
    col("finish_position"),
    current_timestamp().alias("created_at_utc"),
)

# 10. Load
published_event_result_df.write.mode("overwrite").parquet(target_path)

# 11. Release resources
spark.stop()
