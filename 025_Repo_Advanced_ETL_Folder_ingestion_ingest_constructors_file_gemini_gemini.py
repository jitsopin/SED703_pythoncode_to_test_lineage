"""Business purpose: Ingest, clean, and standardize telecom cell tower records.
Source: data/inbound/telecom/towers_20260916.json
Target: telecom_curated.cell_towers (Spark catalog table)
Output grain: One record per physical cell tower.
Business rules: Drop technical URL, rename IDs, append batch date, source system, and processing timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("TelecomTowerIngestion").getOrCreate()
pipeline_source = "tower_registry_api"
pipeline_batch_date = "2026-09-16"

# 2. Define DDL schema and extract raw tower JSON data
tower_schema = "towerId INT, towerRef STRING, providerName STRING, region STRING, specsUrl STRING"
tower_raw_df = spark.read.schema(tower_schema).json("data/inbound/telecom/towers_20260916.json")

# 3. Drop unwanted specification URL column
tower_clean_df = tower_raw_df.drop(col("specsUrl"))

# 4. Rename columns and inject static batch metadata
tower_enriched_df = tower_clean_df.withColumnRenamed("towerId", "tower_id") \
    .withColumnRenamed("towerRef", "tower_code") \
    .withColumn("source_system", lit(pipeline_source)) \
    .withColumn("batch_date", lit(pipeline_batch_date))

# 5. Append ingestion timestamp
tower_final_df = tower_enriched_df.withColumn("processed_at", current_timestamp())

# 6. Persist curated dataset to storage
tower_final_df.write.mode("overwrite").format("parquet").saveAsTable("telecom_curated.cell_towers")

# 7. Release resources
spark.stop()