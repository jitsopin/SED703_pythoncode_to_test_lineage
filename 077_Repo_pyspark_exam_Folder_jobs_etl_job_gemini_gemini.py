"""Business purpose: Transform employee floor data into actionable health/activity metrics.
Source: tests/test_data/employees (Parquet format)
Target: loaded_data (CSV format)
Output grain: One record per employee.
Business rules: Extract employee records, concatenate first and last names, calculate the number of steps required to reach their desk based on their assigned floor, and output the results as a single coalesced CSV file.
Generation profile: Portable PySpark script replacing custom local 'dependencies' wrappers and modularized functions with a self-contained, flat pipeline pattern.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit

# 1. Initialize SparkSession (Replacing custom dependencies.spark.start_spark wrapper)
spark = SparkSession.builder.appName("EmployeeActivityETL").getOrCreate()

# Fallback configuration parameter for steps per floor
steps_per_floor = 21

# 2. Extract raw employee data from the local data lake layer
employees_raw_df = spark.read.parquet("tests/test_data/employees")

# 3. Transform: Format names and calculate activity metrics
employees_transformed_df = employees_raw_df.select(
    col("id"),
    concat_ws(" ", col("first_name"), col("second_name")).alias("name"),
    (col("floor") * lit(steps_per_floor)).alias("steps_to_desk")
)

# 4. Load: Coalesce to a single partition and write to CSV
employees_transformed_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("loaded_data")

# 5. Release resources
spark.stop()