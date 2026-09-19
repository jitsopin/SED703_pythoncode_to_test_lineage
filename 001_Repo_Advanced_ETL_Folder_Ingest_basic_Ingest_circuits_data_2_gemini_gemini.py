"""Business purpose: Extract, standardize, and load motorsport track venues.
Source: data/inbox/motorsport/track_venues.csv
Target: data/cleansed/motorsport/track_venues_parquet
Output grain: One record per racing track venue.
Business rules: Project specific track dimensions, rename geographic coordinates, and add an ingestion timestamp.
Generation profile: Portable PySpark script, flat execution, standard library sources.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession for track ingestion
spark = SparkSession.builder.appName("MotorsportTrackIngestion").getOrCreate()

# 2. Define schema and extract raw track venue data
track_schema = StructType([
    StructField("venue_id", IntegerType(), False),
    StructField("venue_ref", StringType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_location", StringType(), True),
    StructField("nation", StringType(), True),
    StructField("geo_lat", DoubleType(), True),
    StructField("geo_lon", DoubleType(), True),
    StructField("elevation_m", IntegerType(), True),
    StructField("website_url", StringType(), True)
])
track_raw_df = spark.read.option("header", "true").schema(track_schema).csv("data/inbox/motorsport/track_venues.csv")

# 3. Select relevant venue columns and drop unnecessary website URLs
track_selected_df = track_raw_df.select(
    col("venue_id"), col("venue_ref"), col("venue_name"),
    col("city_location"), col("nation"), col("geo_lat"),
    col("geo_lon"), col("elevation_m")
)

# 4. Standardize column names and append processing timestamp
track_standardized_df = track_selected_df.withColumnRenamed("venue_id", "track_id") \
    .withColumnRenamed("venue_ref", "track_code") \
    .withColumnRenamed("city_location", "city") \
    .withColumnRenamed("nation", "country") \
    .withColumnRenamed("geo_lat", "latitude") \
    .withColumnRenamed("geo_lon", "longitude") \
    .withColumnRenamed("elevation_m", "altitude") \
    .withColumn("processed_at", current_timestamp())

# 5. Load processed track venue records to cleansed tier
track_standardized_df.write.mode("overwrite").parquet("data/cleansed/motorsport/track_venues_parquet")

# 6. Release resources
spark.stop()