"""
Business purpose: Demonstrate portable snapshot, partitioned-write, and incremental reconciliation patterns without Delta Lake dependencies.
Sources: JSON competition results and participant reference feeds under data/landing/demo_portable/.
Targets: Parquet result snapshots and a reconciled participant master under data/curated/demo_portable/.
Output grain: Result outputs contain one row per result_id; participant master contains one row per participant_id.
Important rules: Overwrite result snapshots, partition one result copy by team_id, reconcile staged participant batches by participant_id using the latest batch sequence, and overwrite the participant master.
Generation profile: DATAFRAME_CHAIN + PORTABLE_DELTA_DEMO_FLOW
Runtime arguments: None.
Assumptions: Delta-only history, time travel, vacuum, table-conversion, and in-place update/delete demonstrations are intentionally omitted because no portable Spark equivalent preserves those semantics exactly.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, row_number, upper
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_storage_reconciliation_demo").getOrCreate()

result_source_path = "data/landing/demo_portable/result_events.json"
participant_source_path = "data/landing/demo_portable/participant_profiles.json"
result_snapshot_path = "data/curated/demo_portable/result_snapshot"
result_external_path = "data/curated/demo_portable/result_external"
result_partitioned_path = "data/curated/demo_portable/result_partitioned"
participant_master_path = "data/curated/demo_portable/participant_master"

result_schema = StructType([
    StructField("result_id", IntegerType(), False),
    StructField("event_id", IntegerType(), True),
    StructField("participant_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("finish_position", IntegerType(), True),
    StructField("points_awarded", FloatType(), True),
])

participant_schema = StructType([
    StructField("participant_id", IntegerType(), False),
    StructField("birth_date", DateType(), True),
    StructField("given_name", StringType(), True),
    StructField("family_name", StringType(), True),
])

# 2. Read result events
result_raw_df = (
    spark.read
    .schema(result_schema)
    .json(result_source_path)
)

# 3. Build result snapshot
result_snapshot_df = result_raw_df.select(
    col("result_id"),
    col("event_id"),
    col("participant_id"),
    col("team_id"),
    col("finish_position"),
    col("points_awarded"),
    current_timestamp().alias("loaded_at_utc"),
)

# 4. Write managed-style result snapshot
result_snapshot_df.write.mode("overwrite").parquet(result_snapshot_path)

# 5. Write external-style result snapshot
result_snapshot_df.write.mode("overwrite").parquet(result_external_path)

# 6. Write partitioned result snapshot
result_snapshot_df.write.mode("overwrite").partitionBy("team_id").parquet(result_partitioned_path)

# 7. Read participant reference
participant_raw_df = (
    spark.read
    .schema(participant_schema)
    .json(participant_source_path)
)

# 8. Stage day-one participant batch
participant_day1_df = participant_raw_df.filter(
    col("participant_id") <= 10
).select(
    col("participant_id"),
    col("birth_date"),
    col("given_name"),
    col("family_name"),
    lit(1).alias("batch_sequence"),
)

# 9. Stage day-two participant batch
participant_day2_df = participant_raw_df.filter(
    (col("participant_id") >= 6) & (col("participant_id") <= 15)
).select(
    col("participant_id"),
    col("birth_date"),
    upper(col("given_name")).alias("given_name"),
    upper(col("family_name")).alias("family_name"),
    lit(2).alias("batch_sequence"),
)

# 10. Stage day-three participant batch
participant_day3_df = participant_raw_df.filter(
    (col("participant_id") <= 5) |
    ((col("participant_id") >= 16) & (col("participant_id") <= 20))
).select(
    col("participant_id"),
    col("birth_date"),
    upper(col("given_name")).alias("given_name"),
    upper(col("family_name")).alias("family_name"),
    lit(3).alias("batch_sequence"),
)

# 11. Combine staged participant batches
participant_history_df = (
    participant_day1_df
    .unionByName(participant_day2_df)
    .unionByName(participant_day3_df)
)

# 12. Reconcile to latest participant record
participant_rank_spec = Window.partitionBy("participant_id").orderBy(
    col("batch_sequence").desc()
)

participant_reconciled_df = participant_history_df.withColumn(
    "reconciliation_rank",
    row_number().over(participant_rank_spec),
).filter(
    col("reconciliation_rank") == 1
)

# 13. Build participant master projection
participant_master_df = participant_reconciled_df.select(
    col("participant_id"),
    col("birth_date"),
    col("given_name"),
    col("family_name"),
    current_timestamp().alias("updated_at_utc"),
)

# 14. Write participant master
participant_master_df.write.mode("overwrite").parquet(participant_master_path)

# 15. Release resources
spark.stop()
