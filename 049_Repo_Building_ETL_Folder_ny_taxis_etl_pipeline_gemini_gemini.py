"""Business purpose: Process and standardize New York taxi trip metrics.
Source: data/landing/taxis/yellow_tripdata.parquet
Target: Operational Database (JDBC) - public.taxi_trips_curated
Output grain: One record per completed taxi trip.
Business rules: Remove incomplete records, calculate trip duration in minutes, and derive the average speed in miles per hour.
Generation profile: Portable PySpark script replacing Pandas and SQLAlchemy dependencies.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("NYCTaxiETL").getOrCreate()

# 2. Extract raw taxi trips
taxis_raw_df = spark.read.parquet("data/landing/taxis/yellow_tripdata.parquet")

# 3. Transform: Remove records containing null values
taxis_clean_df = taxis_raw_df.na.drop()

# 4. Transform: Calculate trip duration and average speed
taxis_enriched_df = taxis_clean_df.withColumn(
    "trip_duration",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
).withColumn(
    "average_speed",
    col("trip_distance") / (col("trip_duration") / 60.0)
)

# 5. Load: Configure JDBC properties and overwrite target operational table
jdbc_url = "jdbc:postgresql://db-analytical-prod.internal:5432/transport_db"
jdbc_properties = {
    "user": "etl_user",
    "password": "etl_password",
    "driver": "org.postgresql.Driver"
}

taxis_enriched_df.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="public.taxi_trips_curated",
    properties=jdbc_properties
)

# 6. Release resources
spark.stop()