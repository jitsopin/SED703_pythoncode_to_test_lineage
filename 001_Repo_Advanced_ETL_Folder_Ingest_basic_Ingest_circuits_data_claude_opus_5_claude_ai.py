"""
Business purpose: publish a trusted logistics facility profile for the network
planning and distribution reporting teams.
Source: data/landing/logistics/facility_master.csv, a full CSV extract of the
facility master with a header row.
Target: warehouse/reference/logistics/facility_reference, a Parquet snapshot
rebuilt on every run.
Output grain: one row per facility_id.
Business rules: facility_id is mandatory, duplicate facility_id records are
collapsed to a single surviving row, the public site URL is not published to the
reference layer, and each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark reference-dimension ingestion.
Runtime arguments: none.
Assumptions: the landing extract is a complete snapshot delivered as one CSV file.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("logistics_facility_reference_ingestion").getOrCreate()

# 2. Extract the landing snapshot with an explicit schema contract
facility_landing_schema = StructType(fields=[
    StructField("facilityId", IntegerType(), False),
    StructField("facilityCode", StringType(), True),
    StructField("facilityName", StringType(), True),
    StructField("cityName", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("geoLatitude", DoubleType(), True),
    StructField("geoLongitude", DoubleType(), True),
    StructField("elevationMeters", IntegerType(), True),
    StructField("siteUrl", StringType(), True),
])

facility_landing_df = spark.read \
    .option("header", True) \
    .schema(facility_landing_schema) \
    .csv("data/landing/logistics/facility_master.csv")

# 3. Standardize technical column names into business terms through Spark SQL
facility_landing_df.createOrReplaceTempView("vw_facility_landing")

facility_standardized_df = spark.sql("""
    SELECT facilityId        AS facility_id,
           facilityCode      AS facility_code,
           facilityName      AS facility_name,
           cityName          AS city_name,
           countryName       AS country_name,
           geoLatitude       AS latitude,
           geoLongitude      AS longitude,
           elevationMeters   AS elevation_meters
    FROM vw_facility_landing
""")

# 4. Apply business rules: mandatory key, one surviving row per facility
facility_validated_df = facility_standardized_df \
    .filter(col("facility_id").isNotNull()) \
    .dropDuplicates(["facility_id"])

# 5. Build the published projection with the load stamp
facility_reference_df = facility_validated_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("facility_id"),
            col("facility_code"),
            col("facility_name"),
            col("city_name"),
            col("country_name"),
            col("latitude"),
            col("longitude"),
            col("elevation_meters"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the reference layer
facility_reference_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/logistics/facility_reference")

# 7. Release resources
spark.stop()
