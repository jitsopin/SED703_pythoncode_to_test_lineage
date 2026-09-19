"""
Business purpose: keep the subscription member master current by folding each
day's member changes into the published snapshot, so membership reporting always
reads one row per member.
Sources: data/landing/subscription/member_changes.csv, the daily change extract
with a header row, and warehouse/curated/subscription/member_master, the published
snapshot as it stands before this run.
Target: warehouse/curated/subscription/member_master, the same Parquet dataset,
partitioned by region_code and rewritten one partition at a time.
Output grain: one row per member_id.
Business rules: a member present in the change extract replaces the stored row, a
member absent from it keeps the stored row, a member seen for the first time is
added, the partition column is placed last so the written layout matches the
partition definition, and each surviving row carries the timestamp of the load
that last touched it.
Generation profile: portable flat batch PySpark upsert by full outer join.
Runtime arguments: none.
Assumptions: the source file is a helper module with no pipeline of its own, so
the datasets and column set here are mocked around its behaviour; the Delta merge
of the original helper is re-expressed as a full outer join with coalesce because
Delta APIs are not portable; the distinct-value-to-list helper is not materialized
because it yields a driver-side list with no consumer; and the snapshot already
exists, since the helper's first-load branch only creates the table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and replace only the partitions the merge touches
spark = SparkSession.builder.appName("subscription_member_master_upsert").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 2. Extract the daily change file with an explicit schema contract
member_change_landing_schema = StructType(fields=[
    StructField("memberId", IntegerType(), False),
    StructField("planCode", StringType(), True),
    StructField("statusCode", StringType(), True),
    StructField("regionCode", StringType(), False),
    StructField("lastChangeDate", DateType(), True),
])

member_change_landing_df = spark.read \
    .option("header", True) \
    .schema(member_change_landing_schema) \
    .csv("data/landing/subscription/member_changes.csv")

# 3. Standardize business names for the curated layer
member_change_standardized_df = member_change_landing_df.select(
    col("memberId").alias("member_id"),
    col("planCode").alias("plan_code"),
    col("statusCode").alias("status_code"),
    col("regionCode").alias("region_code"),
    col("lastChangeDate").alias("last_change_date"))

# 4. Read the snapshot as it stands, under names that cannot clash on the join
member_master_existing_df = spark.read.parquet("warehouse/curated/subscription/member_master").select(
    col("member_id").alias("existing_member_id"),
    col("plan_code").alias("existing_plan_code"),
    col("status_code").alias("existing_status_code"),
    col("region_code").alias("existing_region_code"),
    col("last_change_date").alias("existing_last_change_date"))

# 5. Match every changed member against the stored snapshot
member_merge_joined_df = member_change_standardized_df.join(
    member_master_existing_df,
    member_change_standardized_df.member_id == member_master_existing_df.existing_member_id,
    "full_outer")

# 6. Let the change win where it exists and keep the stored row otherwise
member_master_updated_df = member_merge_joined_df.select(
    coalesce(col("member_id"), col("existing_member_id")).alias("member_id"),
    coalesce(col("plan_code"), col("existing_plan_code")).alias("plan_code"),
    coalesce(col("status_code"), col("existing_status_code")).alias("status_code"),
    coalesce(col("region_code"), col("existing_region_code")).alias("region_code"),
    coalesce(col("last_change_date"), col("existing_last_change_date")).alias("last_change_date")) \
    .withColumn("ingestion_timestamp", current_timestamp())

# 7. Place the partition column last
member_master_arranged_df = member_master_updated_df.select(
    col("member_id"),
    col("plan_code"),
    col("status_code"),
    col("last_change_date"),
    col("ingestion_timestamp"),
    col("region_code"))

# 8. Write the merged snapshot back, replacing the partitions it carries
member_master_arranged_df.write \
    .mode("overwrite") \
    .partitionBy("region_code") \
    .parquet("warehouse/curated/subscription/member_master")

# 9. Release resources
spark.stop()
