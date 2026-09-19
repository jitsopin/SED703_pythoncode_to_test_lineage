"""
Business purpose: Produce an employee desk-access report by combining employee names and calculating the number of steps required to reach each employee's desk.
Source: Parquet dataset under data/landing/employees.
Target: CSV dataset at data/curated/employees_report.
Output grain: One row per employee containing id, full name, and calculated steps_to_desk.
Important rules: Preserve employee id; concatenate first_name and second_name with a single space into name; calculate steps_to_desk as floor multiplied by steps_per_floor; coalesce to one output partition; overwrite the target CSV with a header.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (CONFIG_DRIVEN_EMPLOYEE_REPORT)
Runtime arguments: steps_per_floor as argv[1].
Assumptions: The project-specific dependencies.spark.start_spark helper, packaged logger, and distributed config file are environment-specific. The business configuration value steps_per_floor is supplied directly as a runtime integer argument.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit

spark = SparkSession.builder.appName("portable_employee_etl").getOrCreate()

if len(sys.argv) < 2:
    raise ValueError("Required argument: steps_per_floor")

steps_per_floor = int(sys.argv[1])

input_path = "data/landing/employees"
output_path = "data/curated/employees_report"

employees_raw_df = (
    spark.read
    .parquet(input_path)
)

employees_transformed_df = employees_raw_df.select(
    col("id"),
    concat_ws(
        " ",
        col("first_name"),
        col("second_name"),
    ).alias("name"),
    (col("floor") * lit(steps_per_floor)).alias("steps_to_desk"),
)

employees_single_partition_df = employees_transformed_df.coalesce(1)

(
    employees_single_partition_df.write
    .mode("overwrite")
    .option("header", True)
    .csv(output_path)
)

spark.stop()
