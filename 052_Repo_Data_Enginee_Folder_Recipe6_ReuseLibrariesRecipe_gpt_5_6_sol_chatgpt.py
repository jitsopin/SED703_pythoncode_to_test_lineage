"""
Business purpose: Generate a small identifier dataset, attach a constant configuration value, derive a calculation from each id, and publish the result as a single-partition CSV dataset.
Source: In-memory Spark range containing ids 0 through 15.
Target: CSV dataset at data/curated/reuse_library_demo/output.
Output grain: One row per generated id.
Important rules: Preserve the 16-row range, attach one constant config_val to every row, derive calc from id, repartition to one partition before writing, and overwrite the portable CSV target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (EXTERNALIZED_LIBRARY_LOGIC)
Runtime arguments: config_value as argv[1] and calc_sql_expression as argv[2].
Assumptions: The source depends on project-specific helper implementations that are not included; their exact semantics cannot be reconstructed safely, so this portable rewrite requires the configuration value and Spark SQL calculation expression at runtime rather than inventing hidden logic.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit

spark = SparkSession.builder.appName("portable_reuse_library_demo").getOrCreate()

if len(sys.argv) < 3:
    raise ValueError(
        "Required arguments: config_value and calc_sql_expression "
        "(example: production 'id * 2')"
    )

config_value = sys.argv[1]
calc_sql_expression = sys.argv[2]
output_path = "data/curated/reuse_library_demo/output"

range_df = spark.range(1 << 4).toDF("id")

configured_df = range_df.withColumn(
    "config_val",
    lit(config_value),
)

calculated_df = configured_df.withColumn(
    "calc",
    expr(calc_sql_expression),
)

single_partition_df = calculated_df.repartition(1)

single_partition_df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
