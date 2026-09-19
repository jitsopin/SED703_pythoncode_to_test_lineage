"""
Business purpose: publish interval meter readings so billing and consumption
reporting share one record per meter interval.
Source: data/landing/metering/interval_readings, a folder of headerless CSV parts
that Spark reads as one dataset.
Target: warehouse/curated/metering/interval_reading_detail, a Parquet snapshot
rebuilt on every run.
Output grain: one row per contract_id, meter_id and interval_index.
Business rules: the three key parts are mandatory, column positions come from the
declared schema because the CSV parts carry no header row, and each load carries
its own ingestion timestamp.
Generation profile: portable flat batch PySpark multi-part CSV folder ingestion.
Runtime arguments: none.
Assumptions: every CSV part in the folder shares the same column order.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("metering_interval_reading_ingestion").getOrCreate()

# 2. Extract every CSV part of the landing folder with an explicit schema contract
interval_reading_landing_schema = StructType(fields=[
    StructField("contractId", IntegerType(), False),
    StructField("meterId", IntegerType(), True),
    StructField("intervalIndex", IntegerType(), True),
    StructField("tariffTier", IntegerType(), True),
    StructField("readingClock", StringType(), True),
    StructField("consumptionWattHours", IntegerType(), True),
])

interval_reading_landing_df = spark.read \
    .option("header", False) \
    .schema(interval_reading_landing_schema) \
    .csv("data/landing/metering/interval_readings")

# 3. Standardize business names for the curated layer
interval_reading_standardized_df = interval_reading_landing_df.select(
    col("contractId").alias("contract_id"),
    col("meterId").alias("meter_id"),
    col("intervalIndex").alias("interval_index"),
    col("tariffTier").alias("tariff_tier"),
    col("readingClock").alias("reading_clock"),
    col("consumptionWattHours").alias("consumption_watt_hours"))

# 4. Apply business rules: every part of the business key is mandatory
interval_reading_eligible_df = interval_reading_standardized_df \
    .filter(col("contract_id").isNotNull()
            & col("meter_id").isNotNull()
            & col("interval_index").isNotNull())

# 5. Build the published projection with the load stamp
interval_reading_detail_df = interval_reading_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("contract_id"),
            col("meter_id"),
            col("interval_index"),
            col("tariff_tier"),
            col("reading_clock"),
            col("consumption_watt_hours"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the curated layer
interval_reading_detail_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/metering/interval_reading_detail")

# 7. Release resources
spark.stop()
