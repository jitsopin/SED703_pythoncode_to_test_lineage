"""
Business purpose: Generate 1,000 synthetic user records with sequential identifiers and append them to a relational users table, then read the target table back for verification.
Source: In-memory Spark range containing 1,000 rows.
Target: JDBC table app.users.
Output grain: One row per generated synthetic user.
Important rules: Generate ids starting at 2; produce name, street, city, and zip columns deterministically from id; append all rows to the relational target; read the target table after the load to preserve the original verification step.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (SYNTHETIC_BULK_INSERT)
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: The original random synthetic-text generator is environment-specific; this portable rewrite preserves the synthetic-data intent and schema using deterministic Spark expressions.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, lpad

spark = SparkSession.builder.appName("portable_synthetic_users_load").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/app"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "app_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

target_table = "app.users"

user_ids_df = spark.range(2, 1002).select(
    col("id").cast("int").alias("id"),
)

synthetic_users_df = user_ids_df.select(
    col("id"),
    concat(lit("User "), col("id")).alias("name"),
    concat(col("id"), lit(" Example Street")).alias("street"),
    concat(lit("City "), (col("id") % 100)).alias("city"),
    lpad((10000 + (col("id") % 90000)).cast("string"), 5, "0").alias("zip"),
)

(
    synthetic_users_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

loaded_users_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

verification_df = loaded_users_df.select(
    col("id"),
    col("name"),
    col("street"),
    col("city"),
    col("zip"),
)

verification_df.show(20, truncate=False)

spark.stop()
