"""Business purpose: Ingest, standardize, and load technical assessment results for HR analytics.
Source: data/landing/hr/technical_assessments (Multiline JSON format).
Target: data/curated/hr/assessments_processed (Parquet format).
Output grain: One record per candidate assessment attempt.
Business rules: Parse multiline JSON, rename internal tracking identifiers, and append processing timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("HRAssessmentIngestion").getOrCreate()

# 2. Define schema and read multiline JSON data
assessment_schema = StructType([
    StructField("assessmentId", IntegerType(), False),
    StructField("campaignId", IntegerType(), True),
    StructField("candidateId", IntegerType(), True),
    StructField("evaluatorId", IntegerType(), True),
    StructField("attemptNumber", IntegerType(), True),
    StructField("finalRank", IntegerType(), True),
    StructField("stage1_score", StringType(), True),
    StructField("stage2_score", StringType(), True),
    StructField("stage3_score", StringType(), True)
])

assessment_raw_df = spark.read.schema(assessment_schema).option("multiLine", True).json("data/landing/hr/technical_assessments")

# 3. Rename identifier columns for downstream standardization
assessment_renamed_df = assessment_raw_df.withColumnRenamed("assessmentId", "assessment_id") \
    .withColumnRenamed("campaignId", "campaign_id") \
    .withColumnRenamed("candidateId", "candidate_id") \
    .withColumnRenamed("evaluatorId", "evaluator_id")

# 4. Append processing timestamp
assessment_final_df = assessment_renamed_df.withColumn("processed_at", current_timestamp())

# 5. Persist curated dataset to storage
assessment_final_df.write.mode("overwrite").parquet("data/curated/hr/assessments_processed")

# 6. Release resources
spark.stop()