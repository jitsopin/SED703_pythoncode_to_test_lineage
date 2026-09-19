"""
Business purpose: Build a portable curated organization-reference table from a date-scoped JSON source.
Source: JSON organization reference data under data/landing/organization_registry/<file_date>/organization_reference.json.
Target: Managed Parquet table analytics_curated.organization_reference_daily.
Output grain: One row per organization_id from the selected file_date feed.
Important rules: Preserve runtime data_source and file_date, read the date-scoped landing path, exclude the source reference URL, standardize identifiers, add ingestion timestamp, and overwrite the curated table.
Generation profile: DATAFRAME_CHAIN + DATE_SCOPED_ORGANIZATION_FLOW
Runtime arguments: First optional argument supplies data_source; second optional argument supplies file_date; defaults are "unspecified" and "2021-03-21".
Assumptions: A Spark catalog supporting saveAsTable is available and the source JSON conforms to the declared schema.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_date_scoped_organization_reference_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
file_date = sys.argv[2] if len(sys.argv) > 2 else "2021-03-21"

source_path = f"data/landing/organization_registry/{file_date}/organization_reference.json"
target_table = "analytics_curated.organization_reference_daily"

organization_schema = StructType([
    StructField("organization_id", IntegerType(), False),
    StructField("organization_code", StringType(), True),
    StructField("organization_name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
organization_raw_df = (
    spark.read
    .schema(organization_schema)
    .json(source_path)
)

# 3. Exclude non-curated source URL
organization_selected_df = organization_raw_df.select(
    col("organization_id"),
    col("organization_code"),
    col("organization_name"),
    col("nationality"),
)

# 4. Attach runtime metadata
organization_runtime_df = organization_selected_df.select(
    col("organization_id"),
    col("organization_code"),
    col("organization_name"),
    col("nationality"),
    lit(data_source).alias("data_source"),
    lit(file_date).alias("file_date"),
)

# 5. Add ingestion metadata
published_organization_df = organization_runtime_df.select(
    col("organization_id"),
    col("organization_code"),
    col("organization_name"),
    col("nationality"),
    col("data_source"),
    col("file_date"),
    current_timestamp().alias("ingested_at_utc"),
)

# 6. Load
published_organization_df.write.mode("overwrite").format("parquet").saveAsTable(target_table)

# 7. Release resources
spark.stop()
