"""
Business purpose: publish a weather station directory so observation and coverage
reporting resolve every reading to one station profile, tagged with the feed it
arrived from.
Source: data/landing/environment/weather_station_export.csv, a full CSV export of
the station registry with a header row.
Target: warehouse/reference/environment/weather_station_directory, a Parquet
snapshot rebuilt on every run.
Output grain: one row per station_id.
Business rules: station_id is mandatory, every row records the feed that supplied
it, the public info URL is not published, and each load carries its own ingestion
timestamp.
Generation profile: portable flat batch PySpark parameterized reference ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the export is a complete snapshot delivered as one CSV file.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("environment_weather_station_directory_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the registry export with an explicit schema contract
weather_station_landing_schema = StructType(fields=[
    StructField("stationId", IntegerType(), False),
    StructField("stationCode", StringType(), True),
    StructField("stationName", StringType(), True),
    StructField("districtName", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("latDegrees", DoubleType(), True),
    StructField("lonDegrees", DoubleType(), True),
    StructField("elevationMeters", IntegerType(), True),
    StructField("infoUrl", StringType(), True),
])

weather_station_landing_df = spark.read \
    .option("header", True) \
    .schema(weather_station_landing_schema) \
    .csv("data/landing/environment/weather_station_export.csv")

# 3. Standardize business names and drop the public info URL
weather_station_standardized_df = weather_station_landing_df.select(
    col("stationId").alias("station_id"),
    col("stationCode").alias("station_code"),
    col("stationName").alias("station_name"),
    col("districtName").alias("district_name"),
    col("countryName").alias("country_name"),
    col("latDegrees").alias("latitude"),
    col("lonDegrees").alias("longitude"),
    col("elevationMeters").alias("elevation_meters"))

# 4. Apply business rules: the station key is mandatory
weather_station_eligible_df = weather_station_standardized_df \
    .filter(col("station_id").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
weather_station_tagged_df = weather_station_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
weather_station_directory_df = weather_station_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("station_id"),
            col("station_code"),
            col("station_name"),
            col("district_name"),
            col("country_name"),
            col("latitude"),
            col("longitude"),
            col("elevation_meters"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the reference layer
weather_station_directory_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/environment/weather_station_directory")

# 8. Release resources
spark.stop()
