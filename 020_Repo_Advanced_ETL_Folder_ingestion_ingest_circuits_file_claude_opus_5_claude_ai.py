"""
Business purpose: publish a cell site directory so coverage and field maintenance
reporting resolve every site to one profile, tagged with the feed it arrived from.
Source: data/landing/network/cell_site_export.csv, a full CSV export of the site
register with a header row.
Target: the managed table network_curated.cell_site_directory, stored as Parquet
and replaced on every run.
Output grain: one row per site_id.
Business rules: site_id is mandatory, every row records the feed that supplied it,
the public info URL is not published, and each load carries its own ingestion
timestamp.
Generation profile: portable flat batch PySpark parameterized reference ingestion
into a managed table.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the export is a complete snapshot delivered as one CSV file and the
target database already exists.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("network_cell_site_directory_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the site export with an explicit schema contract
cell_site_landing_schema = StructType(fields=[
    StructField("siteId", IntegerType(), False),
    StructField("siteRef", StringType(), True),
    StructField("siteName", StringType(), True),
    StructField("localityName", StringType(), True),
    StructField("countryName", StringType(), True),
    StructField("latDegrees", DoubleType(), True),
    StructField("lonDegrees", DoubleType(), True),
    StructField("heightMeters", IntegerType(), True),
    StructField("infoUrl", StringType(), True),
])

cell_site_landing_df = spark.read \
    .option("header", True) \
    .schema(cell_site_landing_schema) \
    .csv("data/landing/network/cell_site_export.csv")

# 3. Standardize business names and drop the public info URL
cell_site_standardized_df = cell_site_landing_df.select(
    col("siteId").alias("site_id"),
    col("siteRef").alias("site_ref"),
    col("siteName").alias("site_name"),
    col("localityName").alias("locality_name"),
    col("countryName").alias("country_name"),
    col("latDegrees").alias("latitude"),
    col("lonDegrees").alias("longitude"),
    col("heightMeters").alias("antenna_height_meters"))

# 4. Apply business rules: the site key is mandatory
cell_site_eligible_df = cell_site_standardized_df \
    .filter(col("site_id").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
cell_site_tagged_df = cell_site_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
cell_site_directory_df = cell_site_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("site_id"),
            col("site_ref"),
            col("site_name"),
            col("locality_name"),
            col("country_name"),
            col("latitude"),
            col("longitude"),
            col("antenna_height_meters"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Replace the managed table with the rebuilt snapshot
cell_site_directory_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("network_curated.cell_site_directory")

# 8. Release resources
spark.stop()
