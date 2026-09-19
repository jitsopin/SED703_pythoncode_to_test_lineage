"""
Business purpose: summarise the membership by region, so branch planning reads how
many members each region holds and how old they are without opening the individual
records.
Source: data/landing/membership/member_records, a folder holding one JSON document
per member.
Target: warehouse/curated/membership/region_profile, a Parquet dataset replaced on
every run.
Output grain: one row per region_name.
Business rules: a member whose region is not recorded is left out of the summary,
each region reports how many members it holds, the average age of those members
rounded to one decimal, and the age of its youngest and oldest member.
Generation profile: portable flat batch PySpark regional aggregation.
Runtime arguments: none.
Assumptions: the source file generates the drop rather than reading anything, so
this pipeline is the consuming side of what it produces and its column set is
taken from the documents that script writes; the folder and the target are named
here; and one document per member is why the count is a row count rather than a
distinct count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("membership_region_profile").getOrCreate()

# 2. Read every document in the drop with an explicit schema contract
member_record_schema = StructType(fields=[
    StructField("memberId", IntegerType(), True),
    StructField("memberName", StringType(), True),
    StructField("memberAge", IntegerType(), True),
    StructField("streetAddress", StringType(), True),
    StructField("cityName", StringType(), True),
    StructField("regionName", StringType(), True),
    StructField("postalCode", StringType(), True),
])

member_record_df = spark.read \
    .schema(member_record_schema) \
    .json("data/landing/membership/member_records")

# 3. Keep the attributes the summary needs, under business names
member_standardized_df = member_record_df.select(
    col("memberId").alias("member_id"),
    col("memberAge").alias("member_age"),
    col("regionName").alias("region_name"))

# 4. Apply business rules: the region must be recorded
member_regional_df = member_standardized_df \
    .filter(col("region_name").isNotNull())

# 5. Summarise the membership of every region
region_profile_df = member_regional_df \
    .groupBy("region_name") \
    .agg(count("*").alias("member_count"),
         spark_round(avg("member_age"), 1).alias("average_age"),
         spark_min("member_age").alias("youngest_age"),
         spark_max("member_age").alias("oldest_age"))

# 6. Replace the regional summary
region_profile_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/membership/region_profile")

# 7. Release resources
spark.stop()
