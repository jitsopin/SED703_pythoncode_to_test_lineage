"""
Business purpose: Run two portable ETL branches: CSV to Parquet, and customer mapping to PostgreSQL.
Sources: data/landing/file_exchange/incoming_records and data/landing/customer_registry/customer_source.
Targets: data/curated/file_exchange/normalized_records and analytics.customer_profile.
Output grain: One output row per input record in each branch.
Important rules: Overwrite Parquet, cast customer_id to integer, preserve names, and overwrite the JDBC target.
Generation profile: DATAFRAME_CHAIN + DUAL_SINK_PORTABLE_FLOW
Runtime arguments: Optional JDBC URL, user, and password.
Assumptions: CSV has a header; customer source contains customer_id, first_name, last_name.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("portable_dual_sink_etl_pipeline").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/analytics"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "analytics_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

csv_source_path = "data/landing/file_exchange/incoming_records"
parquet_target_path = "data/curated/file_exchange/normalized_records"
customer_source_path = "data/landing/customer_registry/customer_source"
customer_target_table = "analytics.customer_profile"

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
])

incoming_records_df = spark.read.option("header", True).option("inferSchema", True).csv(csv_source_path)
incoming_records_df.write.mode("overwrite").parquet(parquet_target_path)

customer_raw_df = spark.read.schema(customer_schema).parquet(customer_source_path)

customer_mapped_df = customer_raw_df.select(
    col("customer_id").cast(IntegerType()).alias("customer_id"),
    col("first_name"),
    col("last_name"),
)

(
    customer_mapped_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", customer_target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
