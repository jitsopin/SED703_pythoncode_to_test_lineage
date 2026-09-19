"""Business purpose: Ingest, clean, and standardize pharmaceutical supplier records.
Source: data/landing/pharma/suppliers.json | Target: data/curated/pharma/suppliers_processed
Generation profile: Flat PySpark script, strict DataFrame boundaries, removed Databricks dependencies."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# 1. Initialize SparkSession and pipeline parameters
spark = SparkSession.builder.appName("PharmaSupplierIngestion").getOrCreate()
pipeline_data_source = "global_pharma_registry"

# 2. Define DDL schema and read raw supplier JSON data
supplier_schema = "supplierId INT, supplierCode STRING, supplierName STRING, originCountry STRING, websiteUrl STRING"
supplier_raw_df = spark.read.schema(supplier_schema).json("data/landing/pharma/suppliers.json")

# 3. Drop unwanted website URL column
supplier_dropped_df = supplier_raw_df.drop(col("websiteUrl"))

# 4. Rename columns, inject data source, and append timestamp
supplier_final_df = supplier_dropped_df.withColumnRenamed("supplierId", "supplier_id") \
    .withColumnRenamed("supplierCode", "supplier_code") \
    .withColumn("data_source", lit(pipeline_data_source)) \
    .withColumn("processed_at", current_timestamp())

# 5. Persist curated dataset to storage
supplier_final_df.write.mode("overwrite").parquet("data/curated/pharma/suppliers_processed")

# 6. Release resources
spark.stop()