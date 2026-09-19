"""
Business purpose: publish a clean supplier registry so purchasing and spend
reporting resolve every order to one supplier profile, tagged with the feed and
the day it arrived from.
Source: data/landing/procurement/supplier_registry, whose daily folders each hold
one JSON extract of the supplier register; the run reads the folder of the
requested day.
Target: the managed table procurement_curated.supplier_master, stored as Parquet
and replaced on every run.
Output grain: one row per supplier_id.
Business rules: supplier_id and supplier_code are mandatory, every row records the
feed and the extract day it came from, the public website is not published, and
each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized dated registry
ingestion into a managed table.
Runtime arguments: argv[1] is the source system name written to source_system and
argv[2] is the extract day written to file_date and used to pick the landing
folder; they fall back to "unspecified" and "2021-03-21".
Assumptions: each daily folder is a complete snapshot of line-delimited JSON
records, the target database already exists, and the table is written as Parquet
because the original wrote Delta, which is not portable.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("procurement_supplier_master_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
file_date = sys.argv[2] if len(sys.argv) > 2 else "2021-03-21"

# 2. Extract the registry of the requested day with an explicit DDL schema contract
supplier_landing_schema = ("supplierId INT, supplierCode STRING, supplierName STRING, "
                           "originCountry STRING, websiteUrl STRING")

supplier_landing_df = spark.read \
    .schema(supplier_landing_schema) \
    .json(f"data/landing/procurement/supplier_registry/{file_date}/suppliers.json")

# 3. Standardize business names and drop the public website
supplier_standardized_df = supplier_landing_df.select(
    col("supplierId").alias("supplier_id"),
    col("supplierCode").alias("supplier_code"),
    col("supplierName").alias("supplier_name"),
    col("originCountry").alias("origin_country"))

# 4. Apply business rules: mandatory key and supplier code
supplier_eligible_df = supplier_standardized_df \
    .filter(col("supplier_id").isNotNull() & col("supplier_code").isNotNull())

# 5. Tag every row with the feed and the extract day named by the arguments
supplier_tagged_df = supplier_eligible_df \
    .withColumn("source_system", lit(source_system)) \
    .withColumn("file_date", lit(file_date))

# 6. Build the published projection with the load stamp
supplier_master_df = supplier_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("supplier_id"),
            col("supplier_code"),
            col("supplier_name"),
            col("origin_country"),
            col("source_system"),
            col("file_date"),
            col("ingestion_timestamp"))

# 7. Replace the managed table with the rebuilt snapshot
supplier_master_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("procurement_curated.supplier_master")

# 8. Release resources
spark.stop()
