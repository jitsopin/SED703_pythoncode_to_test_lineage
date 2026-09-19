"""
Business purpose: Read driver JSON records with an explicit schema and parsing options, normalize the selected fields, and publish a portable Parquet dataset.
Source: JSON driver records at data/landing/driver_json/drivers_1.json.
Target: Parquet dataset at data/curated/driver_json/drivers_normalized.
Output grain: One row per driver record.
Important rules: Parse dob using yyyy-MM-dd, allow single-quoted JSON input, preserve the nested name structure, and overwrite the curated target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE
Runtime arguments: None.
Assumptions: Tutorial-only demonstrations of alternate read syntax, counts, wildcard/directory reads, schema printing, and diagnostic single/multiline examples are excluded because they do not materially feed the final output.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("portable_driver_json_pipeline").getOrCreate()

source_path = "data/landing/driver_json/drivers_1.json"
target_path = "data/curated/driver_json/drivers_normalized"

driver_schema = StructType([
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]), True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

drivers_raw_df = (
    spark.read
    .schema(driver_schema)
    .options(dateFormat="yyyy-MM-dd", allowSingleQuotes=True)
    .json(source_path)
)

drivers_normalized_df = drivers_raw_df.select(
    col("driverId"),
    col("driverRef"),
    col("number"),
    col("code"),
    col("name"),
    col("dob"),
    col("nationality"),
    col("url"),
)

drivers_output_df = drivers_normalized_df.filter(
    col("driverId").isNotNull()
)

drivers_output_df.write.mode("overwrite").parquet(target_path)

spark.stop()
