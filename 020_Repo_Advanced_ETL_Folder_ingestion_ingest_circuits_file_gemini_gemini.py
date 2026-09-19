"""Business purpose: Ingest, standardise, and load physical retail store networks.
Source: data/landing/retail/store_locations.csv
Target: retail_curated.store_locations (Spark catalog table)
Output grain: One record per physical retail store location.
Business rules: Project core fields, standardize naming conventions, flag the source system, and append an ingestion timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("RetailStoreIngestion").getOrCreate()
pipeline_data_source = "corporate_master_data"

# 2. Extract raw store locations
store_schema = StructType([
    StructField("store_internal_id", IntegerType(), False),
    StructField("store_slug", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("city_location", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("geo_lat", DoubleType(), True),
    StructField("geo_lon", DoubleType(), True),
    StructField("store_tier", IntegerType(), True),
    StructField("website_url", StringType(), True)
])

store_raw_df = spark.read.option("header", "true").schema(store_schema).csv("data/landing/retail/store_locations.csv")

# 3. Select relevant location columns
store_selected_df = store_raw_df.select(
    col("store_internal_id"), col("store_slug"), col("branch_name"), 
    col("city_location"), col("country_code"), col("geo_lat"), 
    col("geo_lon"), col("store_tier")
)

# 4. Standardize identifiers and inject source system flag
store_renamed_df = store_selected_df.withColumnRenamed("store_internal_id", "store_id") \
    .withColumnRenamed("store_slug", "store_code") \
    .withColumnRenamed("branch_name", "store_name") \
    .withColumnRenamed("city_location", "city") \
    .withColumnRenamed("country_code", "country") \
    .withColumnRenamed("geo_lat", "latitude") \
    .withColumnRenamed("geo_lon", "longitude") \
    .withColumn("data_source", lit(pipeline_data_source))

# 5. Append ingestion timestamp
store_final_df = store_renamed_df.withColumn("ingested_at", current_timestamp())

# 6. Load data into curated tables
store_final_df.write.mode("overwrite").format("parquet").saveAsTable("retail_curated.store_locations")

# 7. Release resources
spark.stop()