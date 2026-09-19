"""Business purpose: Ingest, standardize, and load port container inspection events.
Source: data/landing/port/container_inspections.json (Multiline JSON format)
Target: data/curated/port/inspections_processed (Parquet format)
Output grain: One record per container inspection event.
Business rules: Parse multiline JSON, rename identifiers, inject source metadata, and append timestamp.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("ContainerInspectionIngestion").getOrCreate()
pipeline_data_source = "port_authority_api"

# 2. Define schema and read multiline JSON data
inspection_schema = StructType([
    StructField("vesselId", IntegerType(), False),
    StructField("inspectorId", IntegerType(), True),
    StructField("inspection_type", StringType(), True),
    StructField("bay_number", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("inspection_duration", StringType(), True),
    StructField("duration_ms", IntegerType(), True)
])
inspection_raw_df = spark.read.schema(inspection_schema).option("multiLine", True).json("data/landing/port/container_inspections.json")

# 3. Add pipeline ingestion timestamp
inspection_timestamped_df = inspection_raw_df.withColumn("ingested_at", current_timestamp())

# 4. Rename identifiers and inject source metadata
inspection_final_df = inspection_timestamped_df.withColumnRenamed("vesselId", "vessel_id") \
    .withColumnRenamed("inspectorId", "inspector_id") \
    .withColumn("data_source", lit(pipeline_data_source))

# 5. Persist curated dataset to storage
inspection_final_df.write.mode("overwrite").parquet("data/curated/port/inspections_processed")

# 6. Release resources
spark.stop()