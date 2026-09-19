"""
Business purpose: publish an airport directory so route planning and flight
reporting resolve every flight to one airport profile, tagged with the feed and
the day it arrived from.
Source: data/landing/aviation/airport_export, whose daily folders each hold one
CSV export of the airport register with a header row; the run reads the folder of
the requested day.
Target: the managed table aviation_curated.airport_directory, stored as Parquet
and replaced on every run.
Output grain: one row per airport_id.
Business rules: airport_id is mandatory, every row records the feed and the export
day it came from, the public info URL is not published, and each load carries its
own ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized dated ingestion into
a managed table.
Runtime arguments: argv[1] is the source system name written to source_system and
argv[2] is the export day written to file_date and used to pick the landing
folder; they fall back to "unspecified" and "2021-03-21".
Assumptions: each daily folder is a complete snapshot delivered as one CSV file,
the target database already exists, and the table is written as Parquet because
the original wrote Delta, which is not portable.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("aviation_airport_directory_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
file_date = sys.argv[2] if len(sys.argv) > 2 else "2021-03-21"

# 2. Extract the export of the requested day with an explicit schema contract
airport_landing_schema = StructType(fields=[
    StructField("airportId", IntegerType(), False),
    StructField("airportRef", StringType(), True),
    StructField("airportName", StringType(), True),
    StructField("cityName", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("latDegrees", DoubleType(), True),
    StructField("lonDegrees", DoubleType(), True),
    StructField("elevationFeet", IntegerType(), True),
    StructField("infoUrl", StringType(), True),
])

airport_landing_df = spark.read \
    .option("header", True) \
    .schema(airport_landing_schema) \
    .csv(f"data/landing/aviation/airport_export/{file_date}/airports.csv")

# 3. Standardize business names and drop the public info URL
airport_standardized_df = airport_landing_df.select(
    col("airportId").alias("airport_id"),
    col("airportRef").alias("airport_ref"),
    col("airportName").alias("airport_name"),
    col("cityName").alias("city_name"),
    col("countryName").alias("country_name"),
    col("latDegrees").alias("latitude"),
    col("lonDegrees").alias("longitude"),
    col("elevationFeet").alias("elevation_feet"))

# 4. Apply business rules: the airport key is mandatory
airport_eligible_df = airport_standardized_df \
    .filter(col("airport_id").isNotNull())

# 5. Tag every row with the feed and the export day named by the arguments
airport_tagged_df = airport_eligible_df \
    .withColumn("source_system", lit(source_system)) \
    .withColumn("file_date", lit(file_date))

# 6. Build the published projection with the load stamp
airport_directory_df = airport_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("airport_id"),
            col("airport_ref"),
            col("airport_name"),
            col("city_name"),
            col("country_name"),
            col("latitude"),
            col("longitude"),
            col("elevation_feet"),
            col("source_system"),
            col("file_date"),
            col("ingestion_timestamp"))

# 7. Replace the managed table with the rebuilt snapshot
airport_directory_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("aviation_curated.airport_directory")

# 8. Release resources
spark.stop()
