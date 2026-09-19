"""
Business purpose: Incrementally compute Hedera native-token account balances from transfer records while carrying forward the latest prior balance per account.
Sources: Native-token transfer Parquet files under data/landing/hedera/native_token_transfer and an optional prior latest-balance snapshot under data/curated/hedera/native_token_balance_latest.
Targets: Incremental native-token balances at data/curated/hedera/native_token_balance and a refreshed latest-balance snapshot at data/curated/hedera/native_token_balance_latest.
Output grain: One deduplicated balance row per created/account_id/consensus_timestamp/block_timestamp_to/transaction_id/cumulative amount combination for the requested range.
Important rules: Restrict transfers to start_nanos..stop_nanos; when prior latest state exists, retain only the most recent row per account before unioning with transfers; compute cumulative SUM(amount) per account ordered by consensus_timestamp; deduplicate multiple transfers within a single transaction by the complete output key; append only rows with consensus_timestamp >= start_nanos; refresh the latest snapshot with the newest row per account.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (INCREMENTAL_ACCOUNT_BALANCE)
Runtime arguments: start_nanos as argv[1], stop_nanos as argv[2], optional prior_latest_path as argv[3].
Assumptions: BigQuery metadata/range discovery and project-specific timestamp helper functions are orchestration concerns. The portable pipeline requires the already-resolved nanosecond range as runtime input and uses local Parquet datasets.
"""

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, sum as spark_sum
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = SparkSession.builder.appName("portable_hedera_native_balance").getOrCreate()

if len(sys.argv) < 3:
    raise ValueError("Required arguments: start_nanos stop_nanos [prior_latest_path]")

start_nanos = int(sys.argv[1])
stop_nanos = int(sys.argv[2])
prior_latest_path = (
    sys.argv[3]
    if len(sys.argv) >= 4
    else "data/curated/hedera/native_token_balance_latest"
)

transfer_input_path = "data/landing/hedera/native_token_transfer"
balance_output_path = "data/curated/hedera/native_token_balance"
latest_output_path = "data/curated/hedera/native_token_balance_latest"

transfer_schema = StructType([
    StructField("created", TimestampType(), True),
    StructField("account_id", StringType(), True),
    StructField("consensus_timestamp", LongType(), True),
    StructField("amount", DecimalType(38, 8), True),
    StructField("block_timestamp_to", LongType(), True),
    StructField("transaction_id", StringType(), True),
])

transfer_raw_df = (
    spark.read
    .schema(transfer_schema)
    .parquet(transfer_input_path)
)

transfer_range_df = transfer_raw_df.filter(
    (col("consensus_timestamp") >= start_nanos)
    & (col("consensus_timestamp") <= stop_nanos)
)

prior_raw_df = (
    spark.read
    .schema(transfer_schema)
    .parquet(prior_latest_path)
)

prior_ranked_df = prior_raw_df.withColumn(
    "_prior_rank",
    row_number().over(
        Window.partitionBy("account_id").orderBy(col("consensus_timestamp").desc())
    ),
)

prior_latest_df = prior_ranked_df.filter(
    col("_prior_rank") == 1
).select(
    col("created"),
    col("account_id"),
    col("consensus_timestamp"),
    col("amount"),
    col("block_timestamp_to"),
    col("transaction_id"),
)

balance_source_df = transfer_range_df.unionByName(
    prior_latest_df
)

balance_running_df = balance_source_df.withColumn(
    "amount",
    spark_sum("amount").over(
        Window.partitionBy("account_id").orderBy(col("consensus_timestamp").asc())
    ),
)

balance_dedup_df = balance_running_df.dropDuplicates([
    "created",
    "account_id",
    "consensus_timestamp",
    "block_timestamp_to",
    "transaction_id",
    "amount",
])

balance_output_df = balance_dedup_df.filter(
    col("consensus_timestamp") >= start_nanos
).select(
    col("created"),
    col("account_id"),
    col("consensus_timestamp"),
    col("amount"),
    col("block_timestamp_to"),
    col("transaction_id"),
)

(
    balance_output_df.write
    .mode("append")
    .parquet(balance_output_path)
)

latest_ranked_df = balance_running_df.withColumn(
    "_latest_rank",
    row_number().over(
        Window.partitionBy("account_id").orderBy(col("consensus_timestamp").desc())
    ),
)

latest_output_df = latest_ranked_df.filter(
    col("_latest_rank") == 1
).select(
    col("created"),
    col("account_id"),
    col("consensus_timestamp"),
    col("amount"),
    col("block_timestamp_to"),
    col("transaction_id"),
)

(
    latest_output_df.write
    .mode("overwrite")
    .parquet(latest_output_path)
)

spark.stop()
