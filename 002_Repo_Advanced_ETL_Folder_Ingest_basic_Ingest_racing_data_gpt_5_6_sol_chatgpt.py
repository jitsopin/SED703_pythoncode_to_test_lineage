"""
Business purpose: Build a portable curated schedule snapshot for motorsport events.
Source: CSV event schedule at data/landing/event_schedule/calendar_feed.csv.
Target: Partitioned Parquet event calendar at data/curated/event_schedule/published_calendar.
Output grain: One row per event_id and season_year from the source schedule.
Important rules: Preserve event sequencing, derive the scheduled start timestamp, stamp load time, and overwrite the published snapshot partitioned by season_year.
Generation profile: DATAFRAME_CHAIN + RACING_SCHEDULE_FLOW
Runtime arguments: None.
Assumptions: Source date and time values follow yyyy-MM-dd and HH:mm:ss conventions, and overwrite remains intended.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_event_calendar_pipeline").getOrCreate()

source_path = "data/landing/event_schedule/calendar_feed.csv"
target_path = "data/curated/event_schedule/published_calendar"

event_schedule_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("season_year", IntegerType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("venue_id", IntegerType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("start_time_text", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
event_schedule_raw_df = (
    spark.read
    .option("header", True)
    .schema(event_schedule_schema)
    .csv(source_path)
)

# 3. Derive timestamps
event_schedule_enriched_df = event_schedule_raw_df.select(
    col("event_id"),
    col("season_year"),
    col("sequence_number"),
    col("venue_id"),
    col("event_name"),
    col("event_date"),
    col("start_time_text"),
    col("reference_url"),
    current_timestamp().alias("loaded_at_utc"),
    to_timestamp(
        concat_ws(" ", col("event_date").cast("string"), col("start_time_text")),
        "yyyy-MM-dd HH:mm:ss",
    ).alias("scheduled_start_timestamp"),
)

# 4. Build published projection
published_event_calendar_df = event_schedule_enriched_df.select(
    col("event_id"),
    col("season_year"),
    col("sequence_number"),
    col("venue_id"),
    col("event_name"),
    col("loaded_at_utc"),
    col("scheduled_start_timestamp"),
)

# 5. Load
published_event_calendar_df.write.mode("overwrite").partitionBy("season_year").parquet(target_path)

# 6. Release resources
spark.stop()
