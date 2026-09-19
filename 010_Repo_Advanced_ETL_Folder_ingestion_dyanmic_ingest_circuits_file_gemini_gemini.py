"""Business purpose: Ingest, filter, and standardize wind farm geospatial data.
Source: data/landing/energy/wind_farms.csv
Target: data/curated/energy/wind_farms_processed (Parquet format)
Output grain: One record per wind farm site.
Business rules: Project core fields, standardize geospatial names, flag data source, and append timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and source parameter
spark = SparkSession.builder.appName("WindFarmDataIngestion").getOrCreate()
pipeline_source_system = "vendor_telemetry_api"

# 2. Define schema and read raw wind farm data
farm_schema = StructType([
    StructField("farmId", IntegerType(), False),
    StructField("farmCode", StringType(), True),
    StructField("plantName", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("geoLat", DoubleType(), True),
    StructField("geoLon", DoubleType(), True),
    StructField("capacityMw", IntegerType(), True),
    StructField("wikiUrl", StringType(), True)
])
farm_raw_df = spark.read.option("header", "true").schema(farm_schema).csv("data/landing/energy/wind_farms.csv")

# 3. Select required geospatial and capacity columns
farm_selected_df = farm_raw_df.select(
    col("farmId"), col("farmCode"), col("plantName"), col("region"), 
    col("country"), col("geoLat"), col("geoLon"), col("capacityMw")
)

# 4. Standardize identifiers and append source system flag
farm_renamed_df = farm_selected_df.withColumnRenamed("farmId", "farm_id") \
    .withColumnRenamed("farmCode", "farm_code") \
    .withColumnRenamed("plantName", "plant_name") \
    .withColumnRenamed("geoLat", "latitude") \
    .withColumnRenamed("geoLon", "longitude") \
    .withColumnRenamed("capacityMw", "capacity_mw") \
    .withColumn("data_source", lit(pipeline_source_system))

# 5. Append pipeline ingestion timestamp
farm_final_df = farm_renamed_df.withColumn("ingested_at", current_timestamp())

# 6. Persist curated records to storage
farm_final_df.write.mode("overwrite").parquet("data/curated/energy/wind_farms_processed")

# 7. Release Spark resources
spark.stop()