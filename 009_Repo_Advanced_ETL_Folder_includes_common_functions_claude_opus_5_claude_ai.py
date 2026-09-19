"""
Business purpose: publish daily store sales so category reporting reloads only the
days present in the incoming extract instead of rewriting the whole history.
Source: data/landing/retail/daily_sales_extract.csv, a dated CSV extract with a
header row.
Target: warehouse/curated/retail/daily_store_sales, a Parquet dataset partitioned
by sales_date and overwritten one partition at a time.
Output grain: one row per store_id, sales_date and product_category.
Business rules: every load carries its own ingestion timestamp, the partition
column is placed last so the written layout matches the partition definition, and
only the partitions present in the extract are replaced.
Generation profile: portable flat batch PySpark dynamic partition overwrite load.
Runtime arguments: none.
Assumptions: the source file is a helper module with no pipeline of its own, so the
source dataset, target dataset and column set here are mocked around its behaviour;
the partition-column reordering is applied to the full column list, correcting the
original helper which returned inside its loop; and the distinct-value-to-list
helper is not materialized because it yields a driver-side list with no consumer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and replace only the partitions present in the extract
spark = SparkSession.builder.appName("retail_daily_store_sales_ingestion").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 2. Extract the dated snapshot with an explicit schema contract
daily_sales_landing_schema = StructType(fields=[
    StructField("storeId", IntegerType(), False),
    StructField("salesDate", DateType(), False),
    StructField("productCategory", StringType(), True),
    StructField("transactionCount", IntegerType(), True),
    StructField("grossAmount", DoubleType(), True),
    StructField("returnAmount", DoubleType(), True),
])

daily_sales_landing_df = spark.read \
    .option("header", True) \
    .schema(daily_sales_landing_schema) \
    .csv("data/landing/retail/daily_sales_extract.csv")

# 3. Standardize business names for the curated layer
daily_sales_standardized_df = daily_sales_landing_df.select(
    col("storeId").alias("store_id"),
    col("salesDate").alias("sales_date"),
    col("productCategory").alias("product_category"),
    col("transactionCount").alias("transaction_count"),
    col("grossAmount").alias("gross_amount"),
    col("returnAmount").alias("return_amount"))

# 4. Stamp the load, inlining the shared ingestion-date behaviour
daily_sales_stamped_df = daily_sales_standardized_df \
    .withColumn("ingestion_timestamp", current_timestamp())

# 5. Place the partition column last, inlining the shared reordering behaviour
daily_sales_arranged_df = daily_sales_stamped_df.select(
    col("store_id"),
    col("product_category"),
    col("transaction_count"),
    col("gross_amount"),
    col("return_amount"),
    col("ingestion_timestamp"),
    col("sales_date"))

# 6. Load, replacing only the partitions carried by this run
daily_sales_arranged_df.write \
    .mode("overwrite") \
    .partitionBy("sales_date") \
    .parquet("warehouse/curated/retail/daily_store_sales")

# 7. Release resources
spark.stop()
