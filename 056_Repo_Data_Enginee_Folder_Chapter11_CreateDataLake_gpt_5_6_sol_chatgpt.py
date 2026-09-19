"""
Business purpose: Generate 1,000 synthetic user records for a portable data-lake landing dataset.
Source: In-memory Spark range containing 1,000 sequential user identifiers.
Target: JSON dataset at data/curated/datalake/users.
Output grain: One row per synthetic user.
Important rules: Generate userid values from 1 through 1000; populate name, age, street, city, state, and zip attributes; publish the result as JSON.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (SYNTHETIC_DATALAKE_GENERATOR)
Runtime arguments: None.
Assumptions: The original random text generator is environment-specific and nondeterministic; this portable rewrite preserves the synthetic-data intent, row count, identifier range, and output schema using deterministic Spark expressions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lpad

spark = SparkSession.builder.appName("portable_synthetic_datalake_generator").getOrCreate()

output_path = "data/curated/datalake/users"

user_ids_df = spark.range(1, 1001).select(
    col("id").cast("int").alias("userid"),
)

synthetic_users_df = user_ids_df.select(
    col("userid"),
    concat(lit("User "), col("userid")).alias("name"),
    (18 + (col("userid") % 84)).cast("int").alias("age"),
    concat(col("userid"), lit(" Example Street")).alias("street"),
    concat(lit("City "), (col("userid") % 100)).alias("city"),
    concat(lit("State "), (col("userid") % 50)).alias("state"),
    lpad((10000 + (col("userid") % 90000)).cast("string"), 5, "0").alias("zip"),
)

users_output_df = synthetic_users_df.select(
    col("userid"),
    col("name"),
    col("age"),
    col("street"),
    col("city"),
    col("state"),
    col("zip"),
)

users_output_df.write.mode("overwrite").json(output_path)

spark.stop()
