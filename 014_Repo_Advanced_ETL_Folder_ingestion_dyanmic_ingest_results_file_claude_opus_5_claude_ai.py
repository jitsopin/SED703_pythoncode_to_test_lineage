"""
Business purpose: publish per-attempt exam performance so cohort analysis and
learner progress reporting work from one measured outcome per attempt, tagged with
the feed it arrived from.
Source: data/landing/education/exam_attempt_results.json, a full JSON extract of
completed exam attempts.
Target: warehouse/curated/education/exam_attempt_performance, a Parquet snapshot
partitioned by exam_id and rebuilt on every run.
Output grain: one row per attempt_id.
Business rules: attempt_id and exam_id are mandatory, the internal status code is
not published to the curated layer, every row records the feed that supplied it,
and each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized assessment-fact
ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the extract is a complete snapshot of line-delimited JSON records
already restricted to submitted attempts.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("education_exam_attempt_performance_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the attempt snapshot with an explicit schema contract
exam_attempt_landing_schema = StructType(fields=[
    StructField("attemptId", IntegerType(), False),
    StructField("examId", IntegerType(), True),
    StructField("learnerId", IntegerType(), True),
    StructField("cohortId", IntegerType(), True),
    StructField("seatNumber", IntegerType(), True),
    StructField("startOrder", IntegerType(), True),
    StructField("finalRank", IntegerType(), True),
    StructField("finalRankText", StringType(), True),
    StructField("finalRankOrder", IntegerType(), True),
    StructField("scorePoints", FloatType(), True),
    StructField("questionsAnswered", IntegerType(), True),
    StructField("elapsedTime", StringType(), True),
    StructField("elapsedMilliseconds", IntegerType(), True),
    StructField("fastestSectionId", IntegerType(), True),
    StructField("sectionRank", IntegerType(), True),
    StructField("fastestSectionTime", StringType(), True),
    StructField("fastestSectionRate", FloatType(), True),
    StructField("statusCode", StringType(), True),
])

exam_attempt_landing_df = spark.read \
    .schema(exam_attempt_landing_schema) \
    .json("data/landing/education/exam_attempt_results.json")

# 3. Standardize business names and drop the internal status code
exam_attempt_standardized_df = exam_attempt_landing_df.select(
    col("attemptId").alias("attempt_id"),
    col("examId").alias("exam_id"),
    col("learnerId").alias("learner_id"),
    col("cohortId").alias("cohort_id"),
    col("seatNumber").alias("seat_number"),
    col("startOrder").alias("start_order"),
    col("finalRank").alias("final_rank"),
    col("finalRankText").alias("final_rank_text"),
    col("finalRankOrder").alias("final_rank_order"),
    col("scorePoints").alias("score_points"),
    col("questionsAnswered").alias("questions_answered"),
    col("elapsedTime").alias("elapsed_time"),
    col("elapsedMilliseconds").alias("elapsed_milliseconds"),
    col("fastestSectionId").alias("fastest_section_id"),
    col("sectionRank").alias("section_rank"),
    col("fastestSectionTime").alias("fastest_section_time"),
    col("fastestSectionRate").alias("fastest_section_rate"))

# 4. Apply business rules: mandatory key and partition column
exam_attempt_eligible_df = exam_attempt_standardized_df \
    .filter(col("attempt_id").isNotNull() & col("exam_id").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
exam_attempt_tagged_df = exam_attempt_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
exam_attempt_performance_df = exam_attempt_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("attempt_id"),
            col("exam_id"),
            col("learner_id"),
            col("cohort_id"),
            col("seat_number"),
            col("start_order"),
            col("final_rank"),
            col("final_rank_text"),
            col("final_rank_order"),
            col("score_points"),
            col("questions_answered"),
            col("elapsed_time"),
            col("elapsed_milliseconds"),
            col("fastest_section_id"),
            col("section_rank"),
            col("fastest_section_time"),
            col("fastest_section_rate"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the curated layer
exam_attempt_performance_df.write \
    .mode("overwrite") \
    .partitionBy("exam_id") \
    .parquet("warehouse/curated/education/exam_attempt_performance")

# 8. Release resources
spark.stop()
