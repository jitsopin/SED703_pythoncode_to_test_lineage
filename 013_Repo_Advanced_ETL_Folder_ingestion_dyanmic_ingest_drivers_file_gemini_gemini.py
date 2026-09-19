"""Business purpose: Ingest, standardize, and flatten freelance developer profiles.
Source: data/landing/freelancers/developers.json (JSON format with nested name struct)
Target: data/curated/freelancers/developers_processed (Parquet format)
Output grain: One record per registered freelance developer.
Business rules: Flatten nested name, drop portfolio URL, rename identifiers, inject source metadata, and append timestamp.
Generation profile: Flat portable PySpark script with isolated DataFrame steps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("FreelanceDeveloperIngestion").getOrCreate()
pipeline_data_source = "tech_talent_api"

# 2. Define nested schema and read raw JSON data
name_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])

developer_schema = StructType([
    StructField("developerId", IntegerType(), False),
    StructField("developerRef", StringType(), True),
    StructField("github_id", IntegerType(), True),
    StructField("tech_stack", StringType(), True),
    StructField("name", name_schema, True),
    StructField("birth_date", DateType(), True),
    StructField("country_residence", StringType(), True),
    StructField("portfolio_url", StringType(), True)
])
dev_raw_df = spark.read.schema(developer_schema).json("data/landing/freelancers/developers.json")

# 3. Add pipeline ingestion timestamp
dev_timestamped_df = dev_raw_df.withColumn("ingested_at", current_timestamp())

# 4. Rename identifiers, flatten name struct, and inject source metadata
dev_enriched_df = dev_timestamped_df.withColumnRenamed("developerId", "developer_id") \
    .withColumnRenamed("developerRef", "developer_ref") \
    .withColumn("name", concat(col("name.first_name"), lit(" "), col("name.last_name"))) \
    .withColumn("data_source", lit(pipeline_data_source))

# 5. Drop the unwanted URL column
dev_final_df = dev_enriched_df.drop(col("portfolio_url"))

# 6. Persist curated dataset to storage
dev_final_df.write.mode("overwrite").parquet("data/curated/freelancers/developers_processed")

# 7. Release resources
spark.stop()