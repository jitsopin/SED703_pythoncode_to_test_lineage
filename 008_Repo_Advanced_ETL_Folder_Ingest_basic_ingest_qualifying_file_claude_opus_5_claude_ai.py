"""
Business purpose: publish laboratory batch trial outcomes so quality review and
instrument performance reporting share one record per trial.
Source: data/landing/laboratory/batch_trial_results, a folder of multiline JSON
extracts of completed trials.
Target: warehouse/curated/laboratory/batch_trial_detail, a Parquet snapshot
rebuilt on every run.
Output grain: one row per trial_id.
Business rules: trial_id and batch_id are mandatory, the three timed rounds are
published under explicit business names instead of their source codes, and each
load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark multiline-JSON trial ingestion.
Runtime arguments: none.
Assumptions: the extract is a complete snapshot and a missing round time means the
round was not attempted.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("laboratory_batch_trial_detail_ingestion").getOrCreate()

# 2. Extract the multiline trial snapshot with an explicit schema contract
batch_trial_landing_schema = StructType(fields=[
    StructField("trialId", IntegerType(), False),
    StructField("batchId", IntegerType(), True),
    StructField("analystId", IntegerType(), True),
    StructField("instrumentId", IntegerType(), True),
    StructField("sampleNumber", IntegerType(), True),
    StructField("rankPosition", IntegerType(), True),
    StructField("r1", StringType(), True),
    StructField("r2", StringType(), True),
    StructField("r3", StringType(), True),
])

batch_trial_landing_df = spark.read \
    .schema(batch_trial_landing_schema) \
    .option("multiLine", True) \
    .json("data/landing/laboratory/batch_trial_results")

# 3. Standardize business names, including the three timed rounds
batch_trial_standardized_df = batch_trial_landing_df.select(
    col("trialId").alias("trial_id"),
    col("batchId").alias("batch_id"),
    col("analystId").alias("analyst_id"),
    col("instrumentId").alias("instrument_id"),
    col("sampleNumber").alias("sample_number"),
    col("rankPosition").alias("trial_rank"),
    col("r1").alias("round_one_time"),
    col("r2").alias("round_two_time"),
    col("r3").alias("round_three_time"))

# 4. Apply business rules: mandatory trial key and batch reference
batch_trial_eligible_df = batch_trial_standardized_df \
    .filter(col("trial_id").isNotNull() & col("batch_id").isNotNull())

# 5. Build the published projection with the load stamp
batch_trial_detail_df = batch_trial_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("trial_id"),
            col("batch_id"),
            col("analyst_id"),
            col("instrument_id"),
            col("sample_number"),
            col("trial_rank"),
            col("round_one_time"),
            col("round_two_time"),
            col("round_three_time"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the curated layer
batch_trial_detail_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/laboratory/batch_trial_detail")

# 7. Release resources
spark.stop()
