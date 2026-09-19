"""
Business purpose: Build a portable curated venue-reference table from a date-scoped CSV source.
Source: CSV venue reference data under data/landing/venue_catalog/<file_date>/venue_reference.csv.
Target: Managed Parquet table analytics_curated.venue_reference_daily.
Output grain: One row per venue_id from the selected file_date feed.
Important rules: Preserve runtime data_source and file_date, read the date-scoped landing path, select required venue attributes, standardize identifiers and coordinates, add ingestion timestamp, and overwrite the curated table.
Generation profile: DATAFRAME_CHAIN + DATE_SCOPED_TABLE_FLOW
Runtime arguments: First optional argument supplies data_source; second optional argument supplies file_date; defaults are "unspecified" and "2021-03-21".
Assumptions: A Spark catalog supporting saveAsTable is available and the selected CSV contains a header row matching the declared schema.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_date_scoped_venue_reference_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
file_date = sys.argv[2] if len(sys.argv) > 2 else "2021-03-21"

source_path = f"data/landing/venue_catalog/{file_date}/venue_reference.csv"
target_table = "analytics_curated.venue_reference_daily"

venue_schema = StructType([
    StructField("venue_id", IntegerType(), False),
    StructField("venue_code", StringType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation_meters", IntegerType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
venue_raw_df = (
    spark.read
    .option("header", True)
    .schema(venue_schema)
    .csv(source_path)
)

# 3. Select required venue attributes
venue_selected_df = venue_raw_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters"),
)

# 4. Attach runtime metadata
venue_runtime_df = venue_selected_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters"),
    lit(data_source).alias("data_source"),
    lit(file_date).alias("file_date"),
)

# 5. Add ingestion metadata
published_venue_df = venue_runtime_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters"),
    col("data_source"),
    col("file_date"),
    current_timestamp().alias("ingested_at_utc"),
)

# 6. Load
published_venue_df.write.mode("overwrite").format("parquet").saveAsTable(target_table)

# 7. Release resources
spark.stop()
