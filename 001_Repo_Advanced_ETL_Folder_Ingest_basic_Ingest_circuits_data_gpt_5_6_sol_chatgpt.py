"""
Business purpose: Build a portable curated snapshot of motor-racing venue reference data.
Source: CSV track catalog at data/landing/track_directory/track_catalog.csv.
Target: Parquet venue profile snapshot at data/curated/track_directory/track_profiles.
Output grain: One row per venue_id from the source track catalog.
Important rules: Keep the selected venue attributes, standardize names, and stamp load time.
Generation profile: DATAFRAME_CHAIN + CIRCUIT_REFERENCE_FLOW
Runtime arguments: None.
Assumptions: The source provides one logical record per track_id and overwrite remains intended.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_track_reference_pipeline").getOrCreate()

source_path = "data/landing/track_directory/track_catalog.csv"
target_path = "data/curated/track_directory/track_profiles"

track_catalog_schema = StructType([
    StructField("track_id", IntegerType(), False),
    StructField("track_code", StringType(), True),
    StructField("venue_name", StringType(), True),
    StructField("municipality", StringType(), True),
    StructField("nation", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("elevation_meters", IntegerType(), True),
    StructField("reference_url", StringType(), True),
])

# 2. Extract
landing_track_df = (
    spark.read
    .option("header", True)
    .schema(track_catalog_schema)
    .csv(source_path)
)

# 3. Standardize
curated_track_base_df = landing_track_df.select(
    col("track_id").alias("venue_id"),
    col("track_code").alias("venue_code"),
    col("venue_name"),
    col("municipality").alias("city"),
    col("nation").alias("country"),
    col("latitude"),
    col("longitude"),
    col("elevation_meters").alias("altitude_meters"),
)

# 4. Build final dataset
track_snapshot_df = curated_track_base_df.select(
    col("venue_id"),
    col("venue_code"),
    col("venue_name"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("altitude_meters"),
    current_timestamp().alias("loaded_at_utc"),
)

# 5. Load
track_snapshot_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
