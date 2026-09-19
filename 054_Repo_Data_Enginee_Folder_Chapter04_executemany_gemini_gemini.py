"""Business purpose: Generate synthetic customer demographics for system load testing.
Source: In-memory distributed sequence generator.
Target: Operational PostgreSQL Database (JDBC) - public.users table
Output grain: One synthetic record per customer.
Business rules: Generate exactly 1000 sequential identifiers starting at 2, derive mock PII attributes (name, street, city, zip) deterministically, and load to the operational store.
Generation profile: Portable PySpark script replacing localized Faker iteration and psycopg2 executemany logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("SyntheticCustomerGeneration").getOrCreate()

# 2. Generate base sequence for 1000 records starting at ID 2
users_raw_df = spark.range(2, 1002).withColumnRenamed("id", "user_id")

# 3. Derive synthetic attributes natively (Replacing Python Faker module)
users_derived_df = users_raw_df.select(
    col("user_id").alias("id"),
    expr("concat('Synthetic_User_', user_id)").alias("name"),
    expr("concat(user_id, ' Fake Street')").alias("street"),
    expr("concat('Mock_City_', cast((user_id % 10) as string))").alias("city"),
    expr("cast(90000 + user_id as string)").alias("zip")
)

# 4. Configure JDBC properties for target PostgreSQL database
jdbc_url = "jdbc:postgresql://localhost:5432/dataengineering"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# 5. Load generated customers to PostgreSQL (Replacing psycopg2 executemany)
users_derived_df.write.mode("append").jdbc(
    url=jdbc_url,
    table="public.users",
    properties=jdbc_properties
)

# 6. Release resources
spark.stop()