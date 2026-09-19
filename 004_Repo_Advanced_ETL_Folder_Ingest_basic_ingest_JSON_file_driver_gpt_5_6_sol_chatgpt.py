"""
Business purpose: Build a portable curated participant registry from a nested JSON reference feed.
Source: JSON participant reference data at data/landing/participant_registry/participant_profiles.json.
Target: Curated Parquet registry at data/curated/participant_registry/published_participants.
Output grain: One row per participant_id from the source reference feed.
Important rules: Flatten the nested personal name, standardize identifiers, exclude the source link, add ingestion time, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + DRIVER_PROFILE_FLOW
Runtime arguments: None.
Assumptions: The source JSON is readable by Spark with the declared nested schema and overwrite remains intended.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_participant_registry_pipeline").getOrCreate()

source_path = "data/landing/participant_registry/participant_profiles.json"
target_path = "data/curated/participant_registry/published_participants"

personal_name_schema = StructType([
    StructField("given_name", StringType(), True),
    StructField("family_name", StringType(), True),
])

participant_schema = StructType([
    StructField("participant_id", IntegerType(), False),
    StructField("participant_key", StringType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("short_code", StringType(), True),
    StructField("personal_name", personal_name_schema, True),
    StructField("birth_date", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("source_link", StringType(), True),
])

# 2. Extract
participant_profile_raw_df = (
    spark.read
    .schema(participant_schema)
    .json(source_path)
)

# 3. Flatten and standardize
participant_profile_flattened_df = participant_profile_raw_df.select(
    col("participant_id"),
    col("participant_key"),
    col("car_number"),
    col("short_code"),
    concat(
        col("personal_name.given_name"),
        lit(" "),
        col("personal_name.family_name"),
    ).alias("full_name"),
    col("birth_date"),
    col("nationality"),
    col("source_link"),
)

# 4. Build curated snapshot
participant_registry_snapshot_df = participant_profile_flattened_df.select(
    col("participant_id"),
    col("participant_key"),
    col("car_number"),
    col("short_code"),
    col("full_name"),
    col("birth_date"),
    col("nationality"),
    current_timestamp().alias("ingested_at_utc"),
)

# 5. Load
participant_registry_snapshot_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
