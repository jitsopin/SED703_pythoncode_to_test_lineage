"""
Business purpose: land the two chef register drops in the curated layer with one
display name per chef and a record of which drop each row came from, so rota
reporting can trace a row back to the file that carried it.
Source: data/landing/culinary/chef_register_part1.json and
data/landing/culinary/chef_register_part2.json, the two halves of the register,
read together as one dataset.
Target: the table culinary_curated.chef_profile, stored as Delta, partitioned by
home_country and replaced on every run.
Output grain: one row per chef_id.
Business rules: chef_id is mandatory, the two parts of the name are joined into one
display name with a single space, the drop a row came from is recorded, the public
profile URL is not published, and each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark multi file ingestion into a
partitioned Delta table.
Runtime arguments: none.
Assumptions: the target database already exists and the Delta format is available
to the session; the original demonstrated a dozen ways of reading the same
register and only its last read reached a write, so its named list of two drops is
the read used here; and its write went to a placeholder location, so a named
target is used instead.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, input_file_name, trim
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("culinary_chef_profile_ingestion").getOrCreate()

# 2. Declare the contract both drops follow
chef_name_schema = StructType(fields=[
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
])

chef_landing_schema = StructType(fields=[
    StructField("chefId", IntegerType(), True),
    StructField("chefRef", StringType(), True),
    StructField("badgeNumber", IntegerType(), True),
    StructField("initialsCode", StringType(), True),
    StructField("personName", chef_name_schema, True),
    StructField("birthDate", DateType(), True),
    StructField("homeCountry", StringType(), True),
    StructField("profileUrl", StringType(), True),
])

# 3. Read both named drops as one dataset
chef_landing_df = spark.read \
    .schema(chef_landing_schema) \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("allowSingleQuotes", True) \
    .json(["data/landing/culinary/chef_register_part1.json",
           "data/landing/culinary/chef_register_part2.json"])

# 4. Join the name parts and record the drop each row came from
chef_named_df = chef_landing_df.select(
    col("chefId").alias("chef_id"),
    col("chefRef").alias("chef_ref"),
    col("badgeNumber").alias("badge_number"),
    col("initialsCode").alias("initials_code"),
    trim(concat_ws(" ", col("personName.givenName"),
                   col("personName.familyName"))).alias("chef_name"),
    col("birthDate").alias("birth_date"),
    col("homeCountry").alias("home_country"),
    input_file_name().alias("source_file"))

# 5. Apply business rules: the chef key is mandatory
chef_eligible_df = chef_named_df \
    .filter(col("chef_id").isNotNull())

# 6. Build the published projection with the load stamp
chef_profile_df = chef_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("chef_id"),
            col("chef_ref"),
            col("badge_number"),
            col("initials_code"),
            col("chef_name"),
            col("birth_date"),
            col("source_file"),
            col("ingestion_timestamp"),
            col("home_country"))

# 7. Replace the curated table, split by home country
chef_profile_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("home_country") \
    .saveAsTable("culinary_curated.chef_profile")

# 8. Release resources
spark.stop()
