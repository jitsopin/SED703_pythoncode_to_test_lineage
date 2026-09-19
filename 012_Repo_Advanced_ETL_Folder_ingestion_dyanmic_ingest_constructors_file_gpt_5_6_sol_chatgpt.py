"""
Business purpose: Build a portable curated organization-reference snapshot from a JSON source.
Source: JSON organization reference data at data/landing/team_registry/team_reference.json.
Target: Curated Parquet snapshot at data/curated/team_registry/published_teams.
Output grain: One row per team_id from the source feed.
Important rules: Exclude the source URL, standardize identifiers, attach runtime data_source, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + DYNAMIC_REFERENCE_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: The source JSON conforms to the declared schema and overwrite remains intended.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_team_registry_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/team_registry/team_reference.json"
target_path = "data/curated/team_registry/published_teams"

team_schema = StructType([
    StructField("team_id", IntegerType(), False),
    StructField("team_code", StringType(), True),
    StructField("team_name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
team_raw_df = (
    spark.read
    .schema(team_schema)
    .json(source_path)
)

# 3. Select curated business attributes
team_selected_df = team_raw_df.select(
    col("team_id"),
    col("team_code"),
    col("team_name"),
    col("nationality"),
)

# 4. Add runtime source metadata
team_sourced_df = team_selected_df.select(
    col("team_id"),
    col("team_code"),
    col("team_name"),
    col("nationality"),
    lit(data_source).alias("data_source"),
)

# 5. Add ingestion metadata
published_team_snapshot_df = team_sourced_df.select(
    col("team_id"),
    col("team_code"),
    col("team_name"),
    col("nationality"),
    col("data_source"),
    current_timestamp().alias("ingested_at_utc"),
)

# 6. Load
published_team_snapshot_df.write.mode("overwrite").parquet(target_path)

# 7. Release resources
spark.stop()
