"""
Business purpose: publish award points for the top ten finishers of each exam so
recognition and cohort reporting read one row per learner and exam, kept current
as each day's attempts arrive.
Sources: the curated tables education_curated.exam_attempts, learners, cohorts and
exams, plus education_presentation.calculated_exam_results as it stands before
this run.
Target: the managed table education_presentation.calculated_exam_results, stored
as Parquet.
Output grain: one row per learner_id and exam_id.
Business rules: only attempts of the requested day and only ranks one to ten are
published, calculated_points is eleven minus the final rank so first place scores
ten, a learner and exam already published has their figures replaced,
created_timestamp is set once when the row first appears, and updated_timestamp is
set only when an existing row is replaced.
Generation profile: portable flat batch PySpark SQL selection with upsert by full
outer join.
Runtime arguments: argv[1] is the attempt day filtered on file_date; it falls back
to "2021-03-21".
Assumptions: the curated tables and the target table already exist; the Delta
merge is re-expressed as a full outer join with coalesce because Delta APIs are
not portable; and the target is read and rewritten in the same run, which Spark
refuses for a table it is also reading, so a runnable deployment needs a staging
target.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, when

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("education_calculated_exam_results_upsert").getOrCreate()
file_date = sys.argv[1] if len(sys.argv) > 1 else "2021-03-21"

# 2. Select the day's top ten finishers with their cohort and exam context
exam_result_updated_df = spark.sql(f"""
    SELECT exams.exam_year,
           cohorts.name AS cohort_name,
           learners.learner_id,
           learners.name AS learner_name,
           exams.exam_id,
           attempts.final_rank,
           attempts.score_points,
           11 - attempts.final_rank AS calculated_points
      FROM education_curated.exam_attempts attempts
      JOIN education_curated.learners learners ON (attempts.learner_id = learners.learner_id)
      JOIN education_curated.cohorts cohorts ON (attempts.cohort_id = cohorts.cohort_id)
      JOIN education_curated.exams exams ON (attempts.exam_id = exams.exam_id)
     WHERE attempts.final_rank <= 10
       AND attempts.file_date = '{file_date}'
""")

# 3. Read the published table as it stands, under names that cannot clash
calculated_exam_existing_df = spark.table("education_presentation.calculated_exam_results").select(
    col("exam_year").alias("existing_exam_year"),
    col("cohort_name").alias("existing_cohort_name"),
    col("learner_id").alias("existing_learner_id"),
    col("learner_name").alias("existing_learner_name"),
    col("exam_id").alias("existing_exam_id"),
    col("final_rank").alias("existing_final_rank"),
    col("score_points").alias("existing_score_points"),
    col("calculated_points").alias("existing_calculated_points"),
    col("created_timestamp").alias("existing_created_timestamp"),
    col("updated_timestamp").alias("existing_updated_timestamp"))

# 4. Match the day's rows against the published table on learner and exam
exam_result_merge_joined_df = exam_result_updated_df.join(
    calculated_exam_existing_df,
    (exam_result_updated_df.learner_id == calculated_exam_existing_df.existing_learner_id)
    & (exam_result_updated_df.exam_id == calculated_exam_existing_df.existing_exam_id),
    "full_outer")

# 5. Let the day's figures win, keep the first-seen stamp, stamp replaced rows
calculated_exam_results_df = exam_result_merge_joined_df.select(
    coalesce(col("exam_year"), col("existing_exam_year")).alias("exam_year"),
    coalesce(col("cohort_name"), col("existing_cohort_name")).alias("cohort_name"),
    coalesce(col("learner_id"), col("existing_learner_id")).alias("learner_id"),
    coalesce(col("learner_name"), col("existing_learner_name")).alias("learner_name"),
    coalesce(col("exam_id"), col("existing_exam_id")).alias("exam_id"),
    coalesce(col("final_rank"), col("existing_final_rank")).alias("final_rank"),
    coalesce(col("score_points"), col("existing_score_points")).alias("score_points"),
    coalesce(col("calculated_points"), col("existing_calculated_points")).alias("calculated_points"),
    coalesce(col("existing_created_timestamp"), current_timestamp()).alias("created_timestamp"),
    when(col("learner_id").isNotNull() & col("existing_learner_id").isNotNull(),
         current_timestamp())
    .otherwise(col("existing_updated_timestamp")).alias("updated_timestamp"))

# 6. Replace the published table with the merged result
calculated_exam_results_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("education_presentation.calculated_exam_results")

# 7. Release resources
spark.stop()
