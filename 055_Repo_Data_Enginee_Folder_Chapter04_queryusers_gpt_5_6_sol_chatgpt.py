"""
Business purpose: Read all user records from a relational table, expose a small verification sample, and export the complete dataset to CSV.
Source: JDBC table app.users.
Target: CSV dataset at data/curated/query_users/fromdb.
Output grain: One row per user record from the source table.
Important rules: Read all rows from users, preserve a verification/readback step, and export the full table to CSV.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (JDBC_READ_EXPORT)
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: Cursor rowcount/rownumber details are driver-specific diagnostics and are not material ETL transformations; the portable rewrite preserves the observable data read and CSV export behavior.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("portable_query_users_export").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/app"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "app_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

source_table = "app.users"
output_path = "data/curated/query_users/fromdb"

users_raw_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

verification_df = users_raw_df.limit(4)

verification_df.show(4, truncate=False)

users_export_df = users_raw_df.select(
    *[col(column_name) for column_name in users_raw_df.columns]
)

users_export_df.write.mode("overwrite").option("header", True).csv(output_path)

spark.stop()
