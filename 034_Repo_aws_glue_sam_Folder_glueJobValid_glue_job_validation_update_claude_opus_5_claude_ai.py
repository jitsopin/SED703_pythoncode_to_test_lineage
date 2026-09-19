"""
Business purpose: keep a readable copy of the outlet headcount held in the
operational database, so reporting does not query the operational system directly.
Source: the table crm.franchise_outlet in the operational database, read over JDBC
with a query that returns only the two published columns.
Target: warehouse/raw/franchise/outlet_headcount, written as JSON and replaced on
every run.
Output grain: one row per outlet.
Business rules: only the staff count and the opening timestamp are published, and
both are stored under business names rather than the column names of the
operational system.
Generation profile: portable flat batch PySpark database extract.
Runtime arguments: argv[1] is the JDBC url of the operational database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: credentials are taken from the arguments instead of being written
into the script as the original did; the schema and row count assertions of the
original were checks on a two-row test fixture, not business rules, so they are
not part of this pipeline; and the second flow of the original wrote a single
hard-coded record and had no source, so it is not part of this pipeline either.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("franchise_outlet_headcount_extract").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/operations"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Pull only the published columns out of the operational table
outlet_headcount_landing_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", "SELECT NumberOfStaff, CreatedAt FROM crm.franchise_outlet") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load()

# 3. Store the columns under business names
outlet_headcount_df = outlet_headcount_landing_df.select(
    col("NumberOfStaff").alias("staff_count"),
    col("CreatedAt").alias("opened_timestamp"))

# 4. Replace the raw copy with this run's extract
outlet_headcount_df.write \
    .mode("overwrite") \
    .json("warehouse/raw/franchise/outlet_headcount")

# 5. Release resources
spark.stop()
