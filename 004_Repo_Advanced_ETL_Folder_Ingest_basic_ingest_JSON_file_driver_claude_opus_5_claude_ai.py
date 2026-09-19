"""
Business purpose: publish a flat instructor directory so course scheduling and
certification reporting resolve every class to one instructor profile.
Source: data/reference/people/instructor_profiles.json, a full JSON extract whose
name attribute is a nested object of given and family name.
Target: warehouse/reference/people/instructor_directory, a Parquet snapshot
rebuilt on every run.
Output grain: one row per instructor_id.
Business rules: instructor_id is mandatory, the nested name object is flattened
into one display name that must not be empty, the public profile URL is not
published, and each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark nested-JSON directory ingestion.
Runtime arguments: none.
Assumptions: the profile extract is a complete snapshot of line-delimited JSON
records.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, trim
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("people_instructor_directory_ingestion").getOrCreate()

# 2. Extract the profile snapshot, declaring the nested name object explicitly
instructor_name_schema = StructType(fields=[
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
])

instructor_landing_schema = StructType(fields=[
    StructField("instructorId", IntegerType(), False),
    StructField("instructorHandle", StringType(), True),
    StructField("badgeNumber", IntegerType(), True),
    StructField("shortCode", StringType(), True),
    StructField("fullName", instructor_name_schema),
    StructField("birthDate", DateType(), True),
    StructField("homeCountry", StringType(), True),
    StructField("profileUrl", StringType(), True),
])

instructor_landing_df = spark.read \
    .schema(instructor_landing_schema) \
    .json("data/reference/people/instructor_profiles.json")

# 3. Flatten the nested name and standardize business names
instructor_standardized_df = instructor_landing_df.select(
    col("instructorId").alias("instructor_id"),
    col("instructorHandle").alias("instructor_handle"),
    col("badgeNumber").alias("badge_number"),
    col("shortCode").alias("instructor_code"),
    trim(concat_ws(" ", col("fullName.givenName"), col("fullName.familyName"))).alias("instructor_name"),
    col("birthDate").alias("birth_date"),
    col("homeCountry").alias("home_country"))

# 4. Apply business rules: mandatory key and non-empty display name
instructor_eligible_df = instructor_standardized_df \
    .filter(col("instructor_id").isNotNull() & (col("instructor_name") != ""))

# 5. Build the published projection with the load stamp
instructor_directory_df = instructor_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("instructor_id"),
            col("instructor_handle"),
            col("instructor_code"),
            col("badge_number"),
            col("instructor_name"),
            col("birth_date"),
            col("home_country"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the reference layer
instructor_directory_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/people/instructor_directory")

# 7. Release resources
spark.stop()
