"""
Business purpose: Build a portable curated participant-reference snapshot from a nested JSON source.
Source: JSON participant reference data at data/landing/participant_registry/participant_reference.json.
Target: Curated Parquet snapshot at data/curated/participant_registry/published_participants.
Output grain: One row per participant_id from the source feed.
Important rules: Flatten the nested personal name, standardize identifiers, attach runtime data_source, add ingestion timestamp, exclude the source URL, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_PARTICIPANT_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: The source JSON conforms to the declared nested schema and overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_participant_reference_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/participant_registry/participant_reference.json"
target_path = "data/curated/participant_registry/published_participants"

personal_name_schema = StructType([
    StructField("given_name", StringType(), True),
    StructField("family_name", StringType(), True),
])

participant_schema = StructType([
    StructField("participant_id", IntegerType(), False),
    StructField("participant_code", StringType(), True),
    StructField("vehicle_number", IntegerType(), True),
    StructField("short_code", StringType(), True),
    StructField("personal_name", personal_name_schema, True),
    StructField("birth_date", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
participant_raw_df = (
    spark.read
    .schema(participant_schema)
    .json(source_path)
)

# 3. Add ingestion metadata
participant_ingested_df = participant_raw_df.select(
    col("participant_id"),
    col("participant_code"),
    col("vehicle_number"),
    col("short_code"),
    col("personal_name"),
    col("birth_date"),
    col("nationality"),
    col("reference_url"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Flatten name and attach runtime source
participant_enriched_df = participant_ingested_df.select(
    col("participant_id"),
    col("participant_code"),
    col("vehicle_number"),
    col("short_code"),
    concat(
        col("personal_name.given_name"),
        lit(" "),
        col("personal_name.family_name"),
    ).alias("full_name"),
    col("birth_date"),
    col("nationality"),
    col("reference_url"),
    col("ingested_at_utc"),
    lit(data_source).alias("data_source"),
)

# 5. Build curated projection
published_participant_snapshot_df = participant_enriched_df.select(
    col("participant_id"),
    col("participant_code"),
    col("vehicle_number"),
    col("short_code"),
    col("full_name"),
    col("birth_date"),
    col("nationality"),
    col("ingested_at_utc"),
    col("data_source"),
)

# 6. Load
published_participant_snapshot_df.write.mode("overwrite").parquet(target_path)

# 7. Release resources
spark.stop()
