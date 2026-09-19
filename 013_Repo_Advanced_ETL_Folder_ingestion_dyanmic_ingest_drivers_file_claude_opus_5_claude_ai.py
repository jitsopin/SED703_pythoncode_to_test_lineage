"""
Business purpose: publish a flat artist roster so exhibition and catalogue
reporting resolve every artwork to one artist profile, tagged with the feed it
arrived from.
Source: data/landing/gallery/artist_profiles.json, a full JSON extract whose name
attribute is a nested object of first and last name.
Target: warehouse/reference/gallery/artist_roster, a Parquet snapshot rebuilt on
every run.
Output grain: one row per artist_id.
Business rules: artist_id is mandatory, the nested name object is flattened into
one display name that must not be empty, every row records the feed that supplied
it, the portfolio URL is not published, and each load carries its own ingestion
timestamp.
Generation profile: portable flat batch PySpark parameterized nested-JSON roster
ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the profile extract is a complete snapshot of line-delimited JSON
records.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, trim
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("gallery_artist_roster_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the profile snapshot, declaring the nested name object explicitly
artist_name_schema = StructType(fields=[
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
])

artist_landing_schema = StructType(fields=[
    StructField("artistId", IntegerType(), False),
    StructField("artistSlug", StringType(), True),
    StructField("catalogNumber", IntegerType(), True),
    StructField("initialsCode", StringType(), True),
    StructField("personName", artist_name_schema),
    StructField("birthDate", DateType(), True),
    StructField("originCountry", StringType(), True),
    StructField("portfolioUrl", StringType(), True),
])

artist_landing_df = spark.read \
    .schema(artist_landing_schema) \
    .json("data/landing/gallery/artist_profiles.json")

# 3. Flatten the nested name and standardize business names
artist_standardized_df = artist_landing_df.select(
    col("artistId").alias("artist_id"),
    col("artistSlug").alias("artist_slug"),
    col("catalogNumber").alias("catalog_number"),
    col("initialsCode").alias("initials_code"),
    trim(concat_ws(" ", col("personName.firstName"), col("personName.lastName"))).alias("artist_name"),
    col("birthDate").alias("birth_date"),
    col("originCountry").alias("origin_country"))

# 4. Apply business rules: mandatory key and non-empty display name
artist_eligible_df = artist_standardized_df \
    .filter(col("artist_id").isNotNull() & (col("artist_name") != ""))

# 5. Tag every row with the feed named by the runtime argument
artist_tagged_df = artist_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
artist_roster_df = artist_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("artist_id"),
            col("artist_slug"),
            col("initials_code"),
            col("catalog_number"),
            col("artist_name"),
            col("birth_date"),
            col("origin_country"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the reference layer
artist_roster_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/gallery/artist_roster")

# 8. Release resources
spark.stop()
