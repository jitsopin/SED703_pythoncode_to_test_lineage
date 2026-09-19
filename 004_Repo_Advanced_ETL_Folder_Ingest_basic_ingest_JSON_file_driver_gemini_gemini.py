"""Business purpose: Ingest, standardize, and flatten delivery courier profiles.
Source: data/landing/logistics/couriers.json (JSON format with nested name struct)
Target: data/curated/logistics/couriers_processed (Parquet format)
Output grain: One record per delivery courier.
Business rules: Flatten nested name, drop profile URL, rename identifiers, and append processing timestamp.
Generation profile: Flat portable PySpark script with isolated DataFrame steps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("CourierProfileIngestion").getOrCreate()

# 2. Define nested schema and extract raw JSON data
name_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True)
])

courier_schema = StructType([
    StructField("courier_id", IntegerType(), False),
    StructField("courier_slug", StringType(), True),
    StructField("fleet_number", IntegerType(), True),
    StructField("dispatch_code", StringType(), True),
    StructField("courier_name", name_schema, True),
    StructField("birth_date", DateType(), True),
    StructField("citizenship", StringType(), True),
    StructField("profile_url", StringType(), True)
])
courier_raw_df = spark.read.schema(courier_schema).json("data/landing/logistics/couriers.json")

# 3. Rename identifiers, overwrite nested name with flat string, and append timestamp
courier_enriched_df = courier_raw_df.withColumnRenamed("courier_id", "staff_id") \
    .withColumnRenamed("courier_slug", "staff_ref") \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("courier_name", concat(col("courier_name.first_name"), lit(" "), col("courier_name.last_name")))

# 4. Drop the unwanted URL column
courier_final_df = courier_enriched_df.drop(col("profile_url"))

# 5. Persist curated dataset to storage
courier_final_df.write.mode("overwrite").parquet("data/curated/logistics/couriers_processed")

# 6. Release resources
spark.stop()