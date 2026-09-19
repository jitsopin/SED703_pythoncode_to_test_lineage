"""Business purpose: Perform incremental upsert of customer profiles.
Source 1: warehouse/customers/profile_snapshot (Historical Parquet snapshot).
Source 2: data/landing/customers/daily_updates.json (Daily JSON updates).
Target: warehouse/customers/profile_snapshot_v2 (Updated Parquet snapshot).
Output grain: One record per active customer.
Business rules: Reconcile daily updates against historical snapshot, keeping the latest record based on update timestamp.
Generation profile: Flat portable PySpark script, Delta operations replaced with standard window reconciliation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, row_number
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("CustomerProfileUpsert").getOrCreate()

# 2. Extract historical customer snapshot
history_df = spark.read.parquet("warehouse/customers/profile_snapshot")

# 3. Extract new incremental daily updates
update_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("email_address", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])
updates_raw_df = spark.read.schema(update_schema).json("data/landing/customers/daily_updates.json")

# 4. Standardize updates to match historical schema
updates_standardized_df = updates_raw_df.withColumn("updated_at", current_timestamp())

# 5. Union historical and new records
combined_df = history_df.unionByName(updates_standardized_df, allowMissingColumns=True)

# 6. Reconcile duplicates keeping the most recent record
dedup_window = Window.partitionBy("customer_id").orderBy(desc("updated_at"))
ranked_df = combined_df.withColumn("recency_rank", row_number().over(dedup_window))
reconciled_df = ranked_df.filter(col("recency_rank") == 1).drop("recency_rank")

# 7. Persist updated customer snapshot
reconciled_df.write.mode("overwrite").parquet("warehouse/customers/profile_snapshot_v2")

# 8. Release resources
spark.stop()