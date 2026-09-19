"""Business purpose: Enrich NYC taxi ride data with calculated geographic distances.
Source: data/landing/uber/uber_data_nyc_2016-06_3m_partitioned.csv (CSV format)
Target: warehouse/uber/uber_data_nyc_2016-06_new (Compressed CSV format)
Output grain: One record per taxi trip enriched with point-to-point mileage and kilometer distances.
Business rules: Extract raw taxi data, apply the Spherical Law of Cosines to calculate the distance between pickup and dropoff coordinates in miles, convert the result to kilometers, and persist the dataset in 4 distributed partitions.
Generation profile: Portable PySpark script replacing local Python module dependencies and slow row-by-row Python UDFs with native JVM-executed SQL Math functions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import acos, col, cos, radians, round, sin

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("UberGeoDistanceETL").getOrCreate()

# 2. Extract raw Uber CSV data
uber_raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/landing/uber/uber_data_nyc_2016-06_3m_partitioned.csv")

# 3. Transform: Calculate geographic distance (Replacing custom Python UDFs with native SQL math)
# Spherical Law of Cosines: d = acos( sin(lat1)*sin(lat2) + cos(lat1)*cos(lat2)*cos(lon2-lon1) ) * R
# Earth Radius (R) in miles = 3958.8

lat1 = radians(col("pickup_latitude"))
lat2 = radians(col("dropoff_latitude"))
lon1 = radians(col("pickup_longitude"))
lon2 = radians(col("dropoff_longitude"))
dlon = lon2 - lon1

# Distance in miles
uber_miles_df = uber_raw_df.withColumn(
    "miles",
    round(
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(dlon)) * 3958.8, 
        2
    )
)

# Distance in kilometers (miles * 1.60934)
uber_kilometers_df = uber_miles_df.withColumn(
    "kilometers",
    round(col("miles") * 1.60934, 2)
)

# 4. Load: Repartition and save to compressed CSV
uber_kilometers_df.repartition(4).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("compression", "gzip") \
    .csv("warehouse/uber/uber_data_nyc_2016-06_new")

# 5. Release resources
spark.stop()