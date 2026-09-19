"""
Business purpose: Build a portable curated venue-reference table from a CSV source.
Source: CSV venue reference data at data/landing/venue_catalog/venue_reference.csv.
Target: Managed Parquet table analytics_curated.venue_reference.
Output grain: One row per venue_id from the source feed.
Important rules: Select required venue attributes, standardize identifiers and coordinates, attach runtime data_source, add ingestion timestamp, and overwrite the curated table.
Generation profile: DATAFRAME_CHAIN + TABLE_REFERENCE_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: A Spark catalog supporting saveAsTable is available at runtime; the source CSV contains a header row and conforms to the declared schema.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_venue_reference_table_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/venue_catalog/venue_reference.csv"
target_table = "analytics_curated.venue_reference"

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

# 3. Select curated attributes
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

# 4. Add runtime source metadata
venue_sourced_df = venue_selected_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters"),
    lit(data_source).alias("data_source"),
)

# 5. Add ingestion metadata
published_venue_df = venue_sourced_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters"),
    col("data_source"),
    current_timestamp().alias("ingested_at_utc"),
)

# 6. Load
published_venue_df.write.mode("overwrite").format("parquet").saveAsTable(target_table)

# 7. Release resources
spark.stop()
