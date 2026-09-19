"""Business purpose: Ingest, standardize, and load warehouse pallet scan events.
Source: data/landing/warehouse/pallet_scans.json (Multiline JSON format)
Target: data/curated/warehouse/pallet_scans_processed (Parquet format)
Output grain: One record per pallet scan event.
Business rules: Parse multiline JSON, rename internal identifiers, and append processing timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("WarehousePalletScanIngestion").getOrCreate()

# 2. Define schema and read multiline JSON data
scan_schema = StructType([
    StructField("warehouseId", IntegerType(), False),
    StructField("operatorId", IntegerType(), True),
    StructField("scan_type", StringType(), True),
    StructField("aisle_number", IntegerType(), True),
    StructField("scan_time", StringType(), True),
    StructField("handling_duration", StringType(), True),
    StructField("handling_ms", IntegerType(), True)
])

scan_raw_df = spark.read.schema(scan_schema).option("multiLine", True).json("data/landing/warehouse/pallet_scans.json")

# 3. Rename identifier columns for standardization
scan_standardized_df = scan_raw_df.withColumnRenamed("warehouseId", "warehouse_id") \
    .withColumnRenamed("operatorId", "operator_id")

# 4. Append ingestion timestamp
scan_final_df = scan_standardized_df.withColumn("processed_at", current_timestamp())

# 5. Write curated dataset to storage
scan_final_df.write.mode("overwrite").parquet("data/curated/warehouse/pallet_scans_processed")

# 6. Release resources
spark.stop()