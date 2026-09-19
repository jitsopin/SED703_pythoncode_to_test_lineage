"""
Business purpose: publish the directorship history of every director, so
governance reporting reads one row per appointment with the person and the
company already resolved.
Sources: the catalogued tables governance_curated.director_profile,
directorship_record and company_registry.
Targets: warehouse/curated/governance/directorship_history, the same history
written as one file in directorship_history_single, and the same history
partitioned by company in directorship_history_by_company; all three are Parquet
and replaced on every run.
Output grain: one row per directorship, that is one person and company
appointment.
Business rules: a directorship must resolve to both a director and a company, so
both joins are inner joins; the alternate names and external identifiers of the
company are not published; and the join keys of the person and company sides are
dropped once the join has served its purpose.
Generation profile: portable flat batch PySpark join and multi-target publish.
Runtime arguments: none.
Assumptions: the catalogued tables already exist; the writes are overwrites so a
rerun is repeatable, where the original would have failed on an existing path; and
the flattening into relational tables at the end of the original is not part of
this pipeline, because it produced a set of tables whose number and names depend
on the data being flattened and so cannot be stated in advance.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark
spark = SparkSession.builder.appName("governance_directorship_history_publish").getOrCreate()

# 2. Read the director side
director_df = spark.table("governance_curated.director_profile").select(
    col("id"),
    col("given_name"),
    col("family_name"),
    col("birth_date"))

# 3. Read the appointment side
directorship_df = spark.table("governance_curated.directorship_record").select(
    col("person_id"),
    col("organization_id"),
    col("role"),
    col("start_date"),
    col("end_date"))

# 4. Read the company side, keeping only the published attributes
company_df = spark.table("governance_curated.company_registry").select(
    col("id").alias("company_id"),
    col("name").alias("company_name"),
    col("classification"))

# 5. Attach every appointment to its director
director_membership_df = director_df.join(
    directorship_df,
    director_df.id == directorship_df.person_id,
    "inner")

# 6. Attach the company and drop the keys the joins consumed
directorship_history_df = director_membership_df.join(
    company_df,
    company_df.company_id == director_membership_df.organization_id,
    "inner") \
    .drop("person_id", "company_id")

# 7. Publish the history
directorship_history_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/governance/directorship_history")

# 8. Gather the same history into a single file
directorship_history_single_df = directorship_history_df.repartition(1)

directorship_history_single_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/governance/directorship_history_single")

# 9. Publish the same history split by company
directorship_history_df.write \
    .mode("overwrite") \
    .partitionBy("company_name") \
    .parquet("warehouse/curated/governance/directorship_history_by_company")

# 10. Release resources
spark.stop()
