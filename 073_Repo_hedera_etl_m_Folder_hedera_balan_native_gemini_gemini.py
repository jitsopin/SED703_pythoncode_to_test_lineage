"""Business purpose: Calculate running native token balances from Hedera network transfers.
Source: data/landing/hedera/native_token_transfers (Parquet format)
Target: warehouse/hedera/native_token_balances (Parquet format)
Output grain: One record per account per transaction timestamp reflecting the new calculated balance.
Business rules: Extract raw token transfers, apply a running sum window function partitioned by account ID ordered by consensus timestamp to calculate the cumulative balance, deduplicate multiple transfers within a single transaction, and load the results to the data warehouse.
Generation profile: Portable PySpark script replacing manual BigQuery API SQL submissions, temporary staging tables, and procedural batching loops.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.window import Window

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("HederaNativeTokenBalanceETL").getOrCreate()

# 2. Extract all native token transfers from the data lake (Replacing targeted BQ partition scans)
transfers_df = spark.read.parquet("data/landing/hedera/native_token_transfers")

# 3. Transform: Apply window function to calculate running balance
# Emulates: SUM(amount) OVER (PARTITION BY account_id ORDER BY consensus_timestamp ASC)
balance_window = Window.partitionBy("account_id").orderBy("consensus_timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_balances_df = transfers_df.withColumn("running_amount", spark_sum("amount").over(balance_window))

# 4. Transform: Deduplicate multiple transfers within a single transaction
# By selecting distinct over the specified columns, we replicate the GROUP BY deduplication
final_balances_df = running_balances_df.select(
    col("created"),
    col("account_id"),
    col("consensus_timestamp"),
    col("block_timestamp_to"),
    col("transaction_id"),
    col("running_amount").alias("amount")
).distinct()

# 5. Load: Sink the calculated balances to the data lake (Replacing BQ INSERT operations and temporary latest_tables)
final_balances_df.write \
    .mode("overwrite") \
    .partitionBy("created") \
    .parquet("warehouse/hedera/native_token_balances")

# 6. Release resources
spark.stop()