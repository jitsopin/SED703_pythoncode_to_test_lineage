"""
Business purpose: keep the volunteer master current by folding each day's profile
changes into the published snapshot, so scheduling and contact reporting always
read one row per volunteer.
Sources: data/landing/community/volunteer_changes.json, the daily change extract
whose name attribute is a nested object, and
warehouse/curated/community/volunteer_master, the published snapshot as it stands
before this run.
Target: warehouse/curated/community/volunteer_master, the same Parquet dataset,
rewritten on every run.
Output grain: one row per volunteer_id.
Business rules: a volunteer present in the change extract has their profile
replaced, a volunteer absent from it keeps the stored row, a volunteer seen for
the first time is added, names are stored upper case, created_timestamp is set
once when the row first appears and never changes, and updated_timestamp is set
only when an existing row is replaced.
Generation profile: portable flat batch PySpark upsert by full outer join.
Runtime arguments: none.
Assumptions: the source file is a Delta feature demo, so only its upsert thread is
built here and the snapshot already exists; the Delta merge is re-expressed as a
full outer join with coalesce because Delta APIs are not portable; the schema is
declared instead of inferred so the types do not depend on the sampled batch; the
identifier ranges of the original were slices used to simulate successive days,
not a business rule; and the audit columns are timestamps because the original
wrote current_timestamp into columns it had declared as dates.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, trim, upper, when
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("community_volunteer_master_upsert").getOrCreate()

# 2. Extract the daily change file, declaring the nested name object explicitly
volunteer_name_schema = StructType(fields=[
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
])

volunteer_change_landing_schema = StructType(fields=[
    StructField("volunteerId", IntegerType(), False),
    StructField("birthDate", DateType(), True),
    StructField("personName", volunteer_name_schema),
])

volunteer_change_landing_df = spark.read \
    .schema(volunteer_change_landing_schema) \
    .json("data/landing/community/volunteer_changes.json")

# 3. Flatten the nested name and store the parts upper case
volunteer_change_standardized_df = volunteer_change_landing_df.select(
    col("volunteerId").alias("volunteer_id"),
    col("birthDate").alias("birth_date"),
    upper(trim(col("personName.firstName"))).alias("first_name"),
    upper(trim(col("personName.lastName"))).alias("last_name"))

# 4. Read the snapshot as it stands, under names that cannot clash on the join
volunteer_master_existing_df = spark.read.parquet("warehouse/curated/community/volunteer_master").select(
    col("volunteer_id").alias("existing_volunteer_id"),
    col("birth_date").alias("existing_birth_date"),
    col("first_name").alias("existing_first_name"),
    col("last_name").alias("existing_last_name"),
    col("created_timestamp").alias("existing_created_timestamp"),
    col("updated_timestamp").alias("existing_updated_timestamp"))

# 5. Match every changed volunteer against the stored snapshot
volunteer_merge_joined_df = volunteer_change_standardized_df.join(
    volunteer_master_existing_df,
    volunteer_change_standardized_df.volunteer_id == volunteer_master_existing_df.existing_volunteer_id,
    "full_outer")

# 6. Let the change win, keep the first-seen stamp, stamp only replaced rows
volunteer_master_updated_df = volunteer_merge_joined_df.select(
    coalesce(col("volunteer_id"), col("existing_volunteer_id")).alias("volunteer_id"),
    coalesce(col("birth_date"), col("existing_birth_date")).alias("birth_date"),
    coalesce(col("first_name"), col("existing_first_name")).alias("first_name"),
    coalesce(col("last_name"), col("existing_last_name")).alias("last_name"),
    coalesce(col("existing_created_timestamp"), current_timestamp()).alias("created_timestamp"),
    when(col("volunteer_id").isNotNull() & col("existing_volunteer_id").isNotNull(),
         current_timestamp())
    .otherwise(col("existing_updated_timestamp")).alias("updated_timestamp"))

# 7. Write the merged snapshot back
volunteer_master_updated_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/community/volunteer_master")

# 8. Release resources
spark.stop()
