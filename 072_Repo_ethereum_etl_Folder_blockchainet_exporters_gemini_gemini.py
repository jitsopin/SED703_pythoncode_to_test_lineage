"""Business purpose: Export Ethereum blockchain transactions to standardized CSV and JSON Data Lake layers.
Source: data/landing/ethereum/transactions (Parquet format)
Target 1: warehouse/ethereum/transactions_csv (CSV format)
Target 2: warehouse/ethereum/transactions_jsonl (JSON Lines format)
Output grain: One record per blockchain transaction.
Business rules: Read standardized transaction data, cast monetary decimal values to 8-point float precision, and perform distributed dual-format writes.
Generation profile: Portable PySpark script replacing localized single-threaded exporter classes, custom encoding functions, and manual JSON/CSV serialization loops.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("BlockchainExportETL").getOrCreate()

# 2. Extract transaction records from landing zone
transactions_raw_df = spark.read.parquet("data/landing/ethereum/transactions")

# 3. Transform: Replicate the 'EncodeDecimal' exporter logic (cast to float, round to 8)
transactions_export_df = transactions_raw_df.select(
    col("transaction_hash"),
    col("from_address"),
    col("to_address"),
    round(col("value").cast("double"), 8).alias("value_eth")
)

# 4. Load: Export to CSV (Replacing CsvItemExporter)
transactions_export_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("warehouse/ethereum/transactions_csv")

# 5. Load: Export to JSON Lines (Replacing JsonLinesItemExporter)
transactions_export_df.write \
    .mode("overwrite") \
    .json("warehouse/ethereum/transactions_jsonl")

# 6. Release resources
spark.stop()