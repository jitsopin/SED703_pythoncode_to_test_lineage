"""Business purpose: Ingest, standardize, and load clinical trial results.
Source: data/landing/clinical/trial_results.json
Target: data/curated/clinical/trial_results_partitioned
Output grain: One record per patient per clinical study.
Business rules: Standardize metric names, inject provider metadata, drop exclusion flags, and partition by study.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and static parameters
spark = SparkSession.builder.appName("ClinicalTrialIngestion").getOrCreate()
pipeline_data_source = "global_health_registry"

# 2. Define schema and extract raw clinical trial results
trial_schema = StructType([
    StructField("resultRecordId", IntegerType(), False),
    StructField("studyId", IntegerType(), True),
    StructField("patientId", IntegerType(), True),
    StructField("clinicId", IntegerType(), True),
    StructField("cohortNumber", IntegerType(), True),
    StructField("baselineScore", IntegerType(), True),
    StructField("finalScore", IntegerType(), True),
    StructField("outcomeCategory", StringType(), True),
    StructField("outcomeRank", IntegerType(), True),
    StructField("improvementMetric", FloatType(), True),
    StructField("visitCount", IntegerType(), True),
    StructField("totalDuration", StringType(), True),
    StructField("durationMs", IntegerType(), True),
    StructField("bestResponseVisit", IntegerType(), True),
    StructField("efficacyRank", IntegerType(), True),
    StructField("bestResponseTime", StringType(), True),
    StructField("maxDosageMg", FloatType(), True),
    StructField("exclusionReason", StringType(), True)
])
trial_raw_df = spark.read.schema(trial_schema).json("data/landing/clinical/trial_results.json")

# 3. Rename columns for standardization and inject data source
trial_renamed_df = trial_raw_df.withColumnRenamed("resultRecordId", "result_record_id") \
    .withColumnRenamed("studyId", "study_id") \
    .withColumnRenamed("patientId", "patient_id") \
    .withColumnRenamed("clinicId", "clinic_id") \
    .withColumnRenamed("outcomeCategory", "outcome_category") \
    .withColumnRenamed("outcomeRank", "outcome_rank") \
    .withColumnRenamed("bestResponseVisit", "best_response_visit") \
    .withColumnRenamed("bestResponseTime", "best_response_time") \
    .withColumnRenamed("maxDosageMg", "max_dosage_mg") \
    .withColumn("data_source", lit(pipeline_data_source))

# 4. Append ingestion timestamp
trial_timestamped_df = trial_renamed_df.withColumn("ingested_at", current_timestamp())

# 5. Drop unwanted exclusion flag column
trial_final_df = trial_timestamped_df.drop(col("exclusionReason"))

# 6. Load curated results partitioned by study
trial_final_df.write.mode("overwrite").partitionBy("study_id").parquet("data/curated/clinical/trial_results_partitioned")

# 7. Release Spark resources
spark.stop()