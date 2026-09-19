"""Business purpose: Ingest, standardize, and load university examination qualifications.
Source: data/landing/education/exam_qualifications.json (Multiline JSON format)
Target: data/curated/education/exam_qualifications_processed (Parquet format)
Output grain: One record per student per term examination.
Business rules: Parse multiline JSON, rename tracking identifiers, inject source metadata, and append processing timestamps.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("UniversityExamIngestion").getOrCreate()
pipeline_data_source = "university_portal_api"

# 2. Define schema and read multiline JSON data
exam_schema = StructType([
    StructField("assessment_record_id", IntegerType(), False),
    StructField("term_id", IntegerType(), True),
    StructField("student_id", IntegerType(), True),
    StructField("faculty_id", IntegerType(), True),
    StructField("registration_number", IntegerType(), True),
    StructField("class_rank", IntegerType(), True),
    StructField("q1_score", StringType(), True),
    StructField("q2_score", StringType(), True),
    StructField("q3_score", StringType(), True)
])
exam_raw_df = spark.read.schema(exam_schema).option("multiLine", True).json("data/landing/education/exam_qualifications.json")

# 3. Add pipeline ingestion timestamp
exam_timestamped_df = exam_raw_df.withColumn("ingested_at", current_timestamp())

# 4. Rename identifiers, append secondary processing timestamp, and inject metadata
exam_final_df = exam_timestamped_df.withColumnRenamed("assessment_record_id", "exam_record_id") \
    .withColumnRenamed("student_id", "candidate_id") \
    .withColumnRenamed("term_id", "academic_term_id") \
    .withColumnRenamed("faculty_id", "university_faculty_id") \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("data_source", lit(pipeline_data_source))

# 5. Persist curated dataset to storage
exam_final_df.write.mode("overwrite").parquet("data/curated/education/exam_qualifications_processed")

# 6. Release resources
spark.stop()