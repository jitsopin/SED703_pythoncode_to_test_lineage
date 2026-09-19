"""
Business purpose: publish per-task picking performance so shift supervisors and
productivity reporting work from one measured outcome per task.
Source: data/landing/operations/pick_task_events.json, a full JSON extract of
completed picking tasks.
Target: warehouse/curated/operations/pick_task_performance, a Parquet snapshot
partitioned by shift_id and rebuilt on every run.
Output grain: one row per task_id.
Business rules: task_id and shift_id are mandatory, the internal status code is
not published to the curated layer, and each load carries its own ingestion
timestamp.
Generation profile: portable flat batch PySpark operational-fact ingestion.
Runtime arguments: none.
Assumptions: the event extract is a complete snapshot of line-delimited JSON
records already restricted to completed tasks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("operations_pick_task_performance_ingestion").getOrCreate()

# 2. Extract the event snapshot with an explicit schema contract
pick_task_landing_schema = StructType(fields=[
    StructField("taskId", IntegerType(), False),
    StructField("shiftId", IntegerType(), True),
    StructField("workerId", IntegerType(), True),
    StructField("stationId", IntegerType(), True),
    StructField("laneNumber", IntegerType(), True),
    StructField("assignedSlot", IntegerType(), True),
    StructField("finishRank", IntegerType(), True),
    StructField("finishRankText", StringType(), True),
    StructField("finishRankOrder", IntegerType(), True),
    StructField("productivityScore", FloatType(), True),
    StructField("unitsPicked", IntegerType(), True),
    StructField("elapsedTime", StringType(), True),
    StructField("elapsedMilliseconds", IntegerType(), True),
    StructField("bestCycleId", IntegerType(), True),
    StructField("cycleRank", IntegerType(), True),
    StructField("bestCycleTime", StringType(), True),
    StructField("bestCycleRate", FloatType(), True),
    StructField("statusCode", StringType(), True),
])

pick_task_landing_df = spark.read \
    .schema(pick_task_landing_schema) \
    .json("data/landing/operations/pick_task_events.json")

# 3. Standardize business names and drop the internal status code
pick_task_standardized_df = pick_task_landing_df.select(
    col("taskId").alias("task_id"),
    col("shiftId").alias("shift_id"),
    col("workerId").alias("worker_id"),
    col("stationId").alias("station_id"),
    col("laneNumber").alias("lane_number"),
    col("assignedSlot").alias("assigned_slot"),
    col("finishRank").alias("finish_rank"),
    col("finishRankText").alias("finish_rank_text"),
    col("finishRankOrder").alias("finish_rank_order"),
    col("productivityScore").alias("productivity_score"),
    col("unitsPicked").alias("units_picked"),
    col("elapsedTime").alias("elapsed_time"),
    col("elapsedMilliseconds").alias("elapsed_milliseconds"),
    col("bestCycleId").alias("best_cycle_id"),
    col("cycleRank").alias("cycle_rank"),
    col("bestCycleTime").alias("best_cycle_time"),
    col("bestCycleRate").alias("best_cycle_rate"))

# 4. Apply business rules: mandatory key and partition column
pick_task_eligible_df = pick_task_standardized_df \
    .filter(col("task_id").isNotNull() & col("shift_id").isNotNull())

# 5. Build the published projection with the load stamp
pick_task_performance_df = pick_task_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("task_id"),
            col("shift_id"),
            col("worker_id"),
            col("station_id"),
            col("lane_number"),
            col("assigned_slot"),
            col("finish_rank"),
            col("finish_rank_text"),
            col("finish_rank_order"),
            col("productivity_score"),
            col("units_picked"),
            col("elapsed_time"),
            col("elapsed_milliseconds"),
            col("best_cycle_id"),
            col("cycle_rank"),
            col("best_cycle_time"),
            col("best_cycle_rate"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the curated layer
pick_task_performance_df.write \
    .mode("overwrite") \
    .partitionBy("shift_id") \
    .parquet("warehouse/curated/operations/pick_task_performance")

# 7. Release resources
spark.stop()
