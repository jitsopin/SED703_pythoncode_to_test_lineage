"""
Business purpose: Build a portable curated event-schedule snapshot from a CSV source.
Source: CSV event schedule at data/landing/event_schedule/event_calendar.csv.
Target: Partitioned Parquet schedule at data/curated/event_schedule/published_event_calendar.
Output grain: One row per event_id from the source feed.
Important rules: Derive the event timestamp from date and time, attach runtime data_source, add ingestion timestamp, project required fields, and overwrite partitioned by season_year.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_SCHEDULE_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: Source date and time values follow yyyy-MM-dd and HH:mm:ss conventions and overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_event_schedule_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/event_schedule/event_calendar.csv"
target_path = "data/curated/event_schedule/published_event_calendar"

event_schedule_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("season_year", IntegerType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("venue_id", IntegerType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_time_text", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
event_schedule_raw_df = (
    spark.read
    .option("header", True)
    .schema(event_schedule_schema)
    .csv(source_path)
)

# 3. Derive schedule timestamp and attach runtime source
event_schedule_enriched_df = event_schedule_raw_df.select(
    col("event_id"),
    col("season_year"),
    col("sequence_number"),
    col("venue_id"),
    col("event_name"),
    col("event_date"),
    col("event_time_text"),
    col("reference_url"),
    to_timestamp(
        concat(col("event_date").cast("string"), lit(" "), col("event_time_text")),
        "yyyy-MM-dd HH:mm:ss",
    ).alias("event_timestamp"),
    lit(data_source).alias("data_source"),
)

# 4. Add ingestion metadata
event_schedule_ingested_df = event_schedule_enriched_df.select(
    col("event_id"),
    col("season_year"),
    col("sequence_number"),
    col("venue_id"),
    col("event_name"),
    col("event_date"),
    col("event_time_text"),
    col("reference_url"),
    col("event_timestamp"),
    col("data_source"),
    current_timestamp().alias("ingested_at_utc"),
)

# 5. Build published projection
published_event_calendar_df = event_schedule_ingested_df.select(
    col("event_id"),
    col("season_year"),
    col("sequence_number"),
    col("venue_id"),
    col("event_name"),
    col("ingested_at_utc"),
    col("event_timestamp"),
)

# 6. Load
published_event_calendar_df.write.mode("overwrite").partitionBy("season_year").parquet(target_path)

# 7. Release resources
spark.stop()
