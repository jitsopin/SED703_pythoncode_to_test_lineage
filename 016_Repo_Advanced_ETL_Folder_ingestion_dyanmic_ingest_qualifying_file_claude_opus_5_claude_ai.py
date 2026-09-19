"""
Business purpose: publish audition outcomes so casting review and agency
performance reporting share one record per audition, tagged with the feed it
arrived from.
Source: data/landing/casting/audition_results, a folder of multiline JSON extracts
of completed auditions.
Target: warehouse/curated/casting/audition_detail, a Parquet snapshot rebuilt on
every run.
Output grain: one row per audition_id.
Business rules: audition_id and production_id are mandatory, the three timed
stages are published under explicit business names instead of their source codes,
every row records the feed that supplied it, and each load carries one ingestion
timestamp.
Generation profile: portable flat batch PySpark parameterized multiline-JSON
audition ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the extract is a complete snapshot, a missing stage time means the
stage was not attempted, and the load stamp is applied once because the original
stamped the same column twice in a row.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("casting_audition_detail_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the multiline audition snapshot with an explicit schema contract
audition_landing_schema = StructType(fields=[
    StructField("auditionId", IntegerType(), False),
    StructField("productionId", IntegerType(), True),
    StructField("performerId", IntegerType(), True),
    StructField("agencyId", IntegerType(), True),
    StructField("entryNumber", IntegerType(), True),
    StructField("rankPosition", IntegerType(), True),
    StructField("s1", StringType(), True),
    StructField("s2", StringType(), True),
    StructField("s3", StringType(), True),
])

audition_landing_df = spark.read \
    .schema(audition_landing_schema) \
    .option("multiLine", True) \
    .json("data/landing/casting/audition_results")

# 3. Standardize business names, including the three timed stages
audition_standardized_df = audition_landing_df.select(
    col("auditionId").alias("audition_id"),
    col("productionId").alias("production_id"),
    col("performerId").alias("performer_id"),
    col("agencyId").alias("agency_id"),
    col("entryNumber").alias("entry_number"),
    col("rankPosition").alias("audition_rank"),
    col("s1").alias("stage_one_time"),
    col("s2").alias("stage_two_time"),
    col("s3").alias("stage_three_time"))

# 4. Apply business rules: mandatory audition key and production reference
audition_eligible_df = audition_standardized_df \
    .filter(col("audition_id").isNotNull() & col("production_id").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
audition_tagged_df = audition_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
audition_detail_df = audition_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("audition_id"),
            col("production_id"),
            col("performer_id"),
            col("agency_id"),
            col("entry_number"),
            col("audition_rank"),
            col("stage_one_time"),
            col("stage_two_time"),
            col("stage_three_time"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the curated layer
audition_detail_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/casting/audition_detail")

# 8. Release resources
spark.stop()
