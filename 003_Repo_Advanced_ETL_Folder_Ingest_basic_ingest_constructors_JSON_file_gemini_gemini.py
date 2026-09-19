"""Business purpose: Ingest, clean, and standardize airline carrier records.
Source: data/landing/aviation/airlines.json (JSON format)
Target: data/curated/aviation/airlines_processed (Parquet format)
Output grain: One record per airline carrier.
Business rules: Drop website URL, rename carrier identifiers, and append processing timestamp.
Generation profile: Flat portable PySpark script with isolated DataFrame steps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("AirlineCarrierIngestion").getOrCreate()

# 2. Define schema and read raw airline JSON data
airline_schema = StructType([
    StructField("airline_id", IntegerType(), False),
    StructField("airline_code", StringType(), True),
    StructField("country_of_origin", StringType(), True),
    StructField("website_url", StringType(), True)
])
airline_raw_df = spark.read.schema(airline_schema).json("data/landing/aviation/airlines.json")

# 3. Drop unwanted URL column
airline_clean_df = airline_raw_df.drop(col("website_url"))

# 4. Rename columns and append ingestion timestamp
airline_final_df = airline_clean_df.withColumnRenamed("airline_id", "carrier_id") \
    .withColumnRenamed("airline_code", "carrier_code") \
    .withColumn("processed_at", current_timestamp())

# 5. Write curated dataset to storage
airline_final_df.write.mode("overwrite").parquet("data/curated/aviation/airlines_processed")

# 6. Release resources
spark.stop()