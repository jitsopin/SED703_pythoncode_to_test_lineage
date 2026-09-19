"""
Business purpose: Demonstrate retry-aware processing by generating a fixed range of records, marking a deliberate failure on the initial attempt, suppressing that failure on retry, and writing the resulting dataset as CSV.
Source: In-memory Spark range from 0 through 1023 with four partitions.
Target: CSV dataset at data/curated/retry_demo/output.
Output grain: One row per generated id.
Important rules: On the initial attempt, id=10 is flagged as a simulated failure; on retry, no rows are flagged. The failure column is materialized so the initial-attempt path raises an exception before a successful write.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (RETRY_FAILURE_DEMO)
Runtime arguments: Optional boolean-like is_retry flag as the first command-line argument (true/false).
Assumptions: The original Glue job detects retries from a provider-specific run identifier; this portable rewrite uses an explicit runtime flag instead.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.appName("portable_retry_failure_demo").getOrCreate()

is_retry = (sys.argv[1].lower() == "true") if len(sys.argv) > 1 else False
output_path = "data/curated/retry_demo/output"

range_df = spark.range(0, 1 << 10, 1, numPartitions=4)

retry_flagged_df = range_df.withColumn(
    "fail",
    lit(False) if is_retry else when(col("id") == 10, lit(True)).otherwise(lit(False)),
)

validated_retry_df = retry_flagged_df.withColumn(
    "failure_guard",
    when(col("fail"), lit(None).cast("int")).otherwise(lit(1)),
)

failing_rows_df = validated_retry_df.filter(col("fail") == lit(True))

assert is_retry or failing_rows_df.count() == 1

output_df = validated_retry_df.drop("failure_guard")

output_df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
