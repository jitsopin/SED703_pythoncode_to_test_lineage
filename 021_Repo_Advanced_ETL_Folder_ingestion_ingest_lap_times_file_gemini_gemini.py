"""Business purpose: Ingest, standardize, and load server performance metrics.
Source: data/landing/infrastructure/server_metrics (Folder of CSV files)
Target: infra_curated.server_metrics (Spark catalog table)
Output grain: One record per server performance measurement.
Business rules: Parse CSV folder, rename internal identifiers, inject source metadata, and append processing timestamps.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and static parameters
spark = SparkSession.builder.appName("ServerMetricsIngestion").getOrCreate()
pipeline_data_source = "infrastructure_monitoring_api"

# 2. Define schema and read CSV directory
metrics_schema = StructType([
    StructField("serverId", IntegerType(), False),
    StructField("applicationId", IntegerType(), True),
    StructField("measurement_sequence", IntegerType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("response_time_str", StringType(), True),
    StructField("response_ms", IntegerType(), True)
])

metrics_raw_df = spark.read.schema(metrics_schema).csv("data/landing/infrastructure/server_metrics")

# 3. Add initial ingestion timestamp
metrics_timestamped_df = metrics_raw_df.withColumn("ingested_at", current_timestamp())

# 4. Rename identifiers, overwrite timestamp, and inject metadata
metrics_final_df = metrics_timestamped_df.withColumnRenamed("serverId", "server_id") \
    .withColumnRenamed("applicationId", "app_id") \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("data_source", lit(pipeline_data_source))

# 5. Persist curated dataset to storage as a table
metrics_final_df.write.mode("overwrite").format("parquet").saveAsTable("infra_curated.server_metrics")

# 6. Release resources
spark.stop()