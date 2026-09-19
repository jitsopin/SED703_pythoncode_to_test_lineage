"""Business purpose: Optimize global customer profiles for data lake querying.
Source: data/landing/customers/global_profiles.csv
Target: Multiple storage zones partitioned for varying analytical access patterns.
Output grain: One record per customer per storage strategy.
Business rules: Demonstrate data distribution strategies using repartitioning by count, hash partitioning by region, and physical directory partitioning.
Generation profile: Portable PySpark script replacing Databricks-specific dbutils and interactive tutorial demonstrations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("CustomerPartitioningOptimization").getOrCreate()

# 2. Define schema and read customer profiles
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("operating_country", StringType(), True)
])
customers_raw_df = spark.read.option("header", "true").schema(customer_schema).csv("data/landing/customers/global_profiles.csv")

# 3. Strategy A: Repartition by fixed number of partitions (distributes data evenly for parallel processing)
customers_uniform_df = customers_raw_df.repartition(4)
customers_uniform_df.write.mode("overwrite").csv("warehouse/customers/uniform_distribution")

# 4. Strategy B: Repartition by specific column (hash partitioning in memory based on country)
customers_hash_df = customers_raw_df.repartition(2, "operating_country")
customers_hash_df.write.mode("overwrite").csv("warehouse/customers/hash_distribution")

# 5. Strategy C: Physical directory partitioning (creates folder hierarchies by country for partition pruning)
customers_raw_df.write.mode("overwrite").partitionBy("operating_country").csv("warehouse/customers/physical_partitioning")

# 6. Release resources
spark.stop()