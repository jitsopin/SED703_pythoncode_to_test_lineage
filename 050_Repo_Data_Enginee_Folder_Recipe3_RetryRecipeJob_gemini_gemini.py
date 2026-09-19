"""Business purpose: Generate synthetic transaction data for downstream testing.
Source: In-memory sequence generator.
Target: warehouse/testing/synthetic_transactions (CSV format)
Output grain: One record per synthetic transaction.
Business rules: Generate a sequence of IDs, flag specific transactions as anomalies, apply a validation score, and persist.
Generation profile: Portable PySpark script replacing AWS Glue dynamic frames, proprietary retry logic, and artificial failure injection.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("SyntheticTransactionGeneration").getOrCreate()

# 2. Generate base transaction sequence (Replacing proprietary job run logic and spark.range)
transactions_raw_df = spark.range(1024, numPartitions=4).withColumnRenamed("id", "transaction_id")

# 3. Derive anomaly flags (Simulating the conditional edge-case logic)
transactions_flagged_df = transactions_raw_df.withColumn(
    "is_anomaly",
    expr("CASE WHEN transaction_id = 10 THEN true ELSE false END")
)

# 4. Apply validation scoring (Replacing the deliberate failure UDF)
transactions_scored_df = transactions_flagged_df.withColumn(
    "validation_score",
    expr("CASE WHEN is_anomaly THEN 0.0 ELSE 100.0 END")
)

# 5. Load generated transactions to storage
transactions_scored_df.write.mode("overwrite").option("header", "true").csv("warehouse/testing/synthetic_transactions")

# 6. Release resources
spark.stop()