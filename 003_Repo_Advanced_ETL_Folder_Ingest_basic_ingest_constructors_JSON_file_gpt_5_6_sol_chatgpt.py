"""
Business purpose: Build a portable curated registry of motorsport organizations from a JSON reference feed.
Source: JSON organization reference data at data/landing/organization_registry/organization_reference.json.
Target: Curated Parquet registry at data/curated/organization_registry/published_organizations.
Output grain: One row per organization_id from the source reference feed.
Important rules: Exclude the source reference URL, standardize identifier names, add ingestion timestamp, and overwrite the curated snapshot.
Generation profile: DATAFRAME_CHAIN + CONSTRUCTOR_REFERENCE_FLOW
Runtime arguments: None.
Assumptions: The source JSON is newline-delimited or otherwise readable by Spark JSON reader with the declared schema.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_organization_registry_pipeline").getOrCreate()

source_path = "data/landing/organization_registry/organization_reference.json"
target_path = "data/curated/organization_registry/published_organizations"

organization_schema = StructType([
    StructField("organization_id", IntegerType(), False),
    StructField("organization_code", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
organization_raw_df = (
    spark.read
    .schema(organization_schema)
    .json(source_path)
)

# 3. Standardize and exclude non-curated metadata
organization_standardized_df = organization_raw_df.select(
    col("organization_id"),
    col("organization_code"),
    col("nationality"),
)

# 4. Add ingestion metadata
organization_snapshot_df = organization_standardized_df.select(
    col("organization_id"),
    col("organization_code"),
    col("nationality"),
    current_timestamp().alias("ingested_at_utc"),
)

# 5. Load
organization_snapshot_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
