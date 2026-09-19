"""
Business purpose: publish a clean registry of record labels so catalogue and
royalty reporting resolve every release to one label profile.
Source: data/reference/catalog/record_label_registry.json, a full JSON extract of
the label registry.
Target: warehouse/reference/catalog/record_label_master, a Parquet snapshot
rebuilt on every run.
Output grain: one row per label_id.
Business rules: label_id and label_code are mandatory, the label code is stored
upper case without padding, the origin country is stored in title case, the public
website is not published, and each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark reference-registry ingestion.
Runtime arguments: none.
Assumptions: the registry extract is a complete snapshot of line-delimited JSON
records.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, initcap, trim, upper
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("catalog_record_label_master_ingestion").getOrCreate()

# 2. Extract the registry snapshot with an explicit schema contract
record_label_landing_schema = StructType(fields=[
    StructField("labelId", IntegerType(), False),
    StructField("labelCode", StringType(), True),
    StructField("originCountry", StringType(), True),
    StructField("websiteUrl", StringType(), True),
])

record_label_landing_df = spark.read \
    .schema(record_label_landing_schema) \
    .json("data/reference/catalog/record_label_registry.json")

# 3. Standardize business names and text formatting, drop the public website
record_label_standardized_df = record_label_landing_df.select(
    col("labelId").alias("label_id"),
    upper(trim(col("labelCode"))).alias("label_code"),
    initcap(trim(col("originCountry"))).alias("origin_country"))

# 4. Apply business rules: mandatory key and mandatory label code
record_label_eligible_df = record_label_standardized_df \
    .filter(col("label_id").isNotNull() & col("label_code").isNotNull())

# 5. Build the published projection with the load stamp
record_label_master_df = record_label_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("label_id"),
            col("label_code"),
            col("origin_country"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the reference layer
record_label_master_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/catalog/record_label_master")

# 7. Release resources
spark.stop()
