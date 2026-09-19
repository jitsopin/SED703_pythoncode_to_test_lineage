"""
Business purpose: take a full extract of the tenant register out of the
operational database, so downstream teams work from a flat file instead of
querying the live system.
Source: the table housing_ops.tenant in the operational database, whose columns
carry the abbreviated names of the legacy system.
Target: data/export/housing/tenant_extract, the whole register written as comma
separated text without a header row, replaced on every run.
Output grain: one row per tenant.
Business rules: the register is taken whole with no rows left out, and every
column is published under a readable name instead of the abbreviation the
operational table uses.
Generation profile: portable flat batch PySpark full table extract.
Runtime arguments: argv[1] is the JDBC url of the operational database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: the original selected every column without naming any, so the column
set here is the register's attributes; its copy wrote rows with a comma separator
and no header, which is how the extract is written; credentials are taken from the
arguments rather than written into the script; and the row counts and the read
back of the file it printed produced nothing.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("housing_tenant_extract").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/operations"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the whole tenant register
tenant_landing_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "housing_ops.tenant") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("tnt_id"),
            col("tnt_nm"),
            col("addr_street"),
            col("addr_city"),
            col("addr_postcode"))

# 3. Publish every column under a readable name
tenant_extract_df = tenant_landing_df.select(
    col("tnt_id").alias("tenant_id"),
    col("tnt_nm").alias("tenant_name"),
    col("addr_street").alias("street_address"),
    col("addr_city").alias("city_name"),
    col("addr_postcode").alias("postal_code"))

# 4. Replace the extract
tenant_extract_df.write \
    .option("header", False) \
    .option("sep", ",") \
    .format("csv") \
    .mode("overwrite") \
    .save("data/export/housing/tenant_extract")

# 5. Release resources
spark.stop()
