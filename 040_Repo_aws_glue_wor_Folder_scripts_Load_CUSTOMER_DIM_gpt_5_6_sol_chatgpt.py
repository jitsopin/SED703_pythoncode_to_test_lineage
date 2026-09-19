"""
Business purpose: Build a denormalized customer dimension by joining customer master records with customer-site records.
Sources: Parquet datasets at data/landing/customer_dimension/customers and data/landing/customer_dimension/customer_sites.
Target: Parquet dataset at data/curated/customer_dimension/customer_dim.
Output grain: One row per matched customer-site combination after removing the shared customer join key.
Important rules: Inner join customer and customer-site records on cust_id, remove cust_id from the published dimension, and overwrite the portable Parquet target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE
Runtime arguments: None.
Assumptions: Both source datasets contain cust_id and expose their remaining business attributes through Parquet schema inference.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("portable_customer_dimension_pipeline").getOrCreate()

customer_path = "data/landing/customer_dimension/customers"
customer_site_path = "data/landing/customer_dimension/customer_sites"
customer_dim_path = "data/curated/customer_dimension/customer_dim"

customer_df = spark.read.parquet(customer_path)

customer_site_df = spark.read.parquet(customer_site_path)

customer_joined_df = customer_df.join(
    customer_site_df,
    on="cust_id",
    how="inner",
)

customer_dim_df = customer_joined_df.drop("cust_id")

customer_dim_df.write.mode("overwrite").parquet(customer_dim_path)

spark.stop()
