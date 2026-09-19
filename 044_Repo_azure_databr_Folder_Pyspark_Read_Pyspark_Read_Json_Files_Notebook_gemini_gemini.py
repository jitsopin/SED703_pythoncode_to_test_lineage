"""Business purpose: Ingest, standardize, and archive multiline driver profiles.
Source: /mnt/practice/read_write_json/multi_line.json
Target: /mnt/practice/archive/drivers_profiles (Parquet format)
Output grain: One record per registered driver.
Business rules: Extract multiline JSON, enforce strict date schema, allow single-quote deviations, and overwrite the existing archive.
Generation profile: Consolidated portable PySpark ETL replacing fragmented tutorial steps.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("DriverProfileArchival").getOrCreate()

# 2. Define expected schema to enforce strict date parsing and complex struct fields
drivers_schema = StructType([
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]), True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# 3. Read multiline JSON applying format tolerance options and explicit schema
driver_profiles_df = spark.read \
    .schema(drivers_schema) \
    .options(dateFormat="yyyy-MM-dd", allowSingleQuotes=True, multiLine=True) \
    .json("/mnt/practice/read_write_json/multi_line.json")

# 4. Save extracted profiles to an overwritten Parquet archive
driver_profiles_df.write \
    .mode("overwrite") \
    .parquet("/mnt/practice/archive/drivers_profiles")

# 5. Release resources
spark.stop()