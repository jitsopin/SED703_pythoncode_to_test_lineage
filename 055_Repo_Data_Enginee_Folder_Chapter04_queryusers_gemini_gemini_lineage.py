"""Business purpose: Export user profiles from an operational database to a data lake.
Source: Operational PostgreSQL Database (JDBC) - public.users table
Target: warehouse/users_export (CSV format)
Output grain: One record per user.
Business rules: Extract the full users table and archive it as a CSV dataset, bypassing local file staging and exploratory fetch commands.
Generation profile: Portable PySpark script replacing psycopg2 connections and cur.copy_to() local file exports.
"""

from pyspark.sql import SparkSession

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("UserExportETL").getOrCreate()

# 2. Configure JDBC properties for target PostgreSQL database
jdbc_url = "jdbc:postgresql://localhost:5432/dataengineering"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# 3. Extract the full users table via PySpark JDBC reader
users_raw_df = spark.read.jdbc(
    url=jdbc_url,
    table="public.users",
    properties=jdbc_properties
)

# 4. Load the extracted dataset to distributed storage as a CSV (Replacing local fromdb.csv export)
users_raw_df.write.mode("overwrite").option("header", "true").csv("warehouse/users_export")

# 5. Release resources
spark.stop()