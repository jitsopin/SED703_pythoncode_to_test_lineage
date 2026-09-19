"""
Business purpose: consolidate the service account drop, which arrives as one small
JSON file per account holder, into a single curated dataset that reporting can
read without opening thousands of files.
Source: data/landing/utility/service_account_records, a folder holding one JSON
document per account holder.
Target: warehouse/curated/utility/service_account, a Parquet dataset written as one
file and replaced on every run.
Output grain: one row per account_number.
Business rules: a record without an account number is not published, an account
that arrives in more than one file is kept once, and the address parts are
published as they arrive rather than being combined.
Generation profile: portable flat batch PySpark small file consolidation.
Runtime arguments: none.
Assumptions: the source file generates the drop rather than reading anything, so
this pipeline is the consuming side of what it produces; its column set is taken
from the documents that script writes, while the folder and the target are named
here; and the deduplication is there because that script names each file after the
holder, so two holders with the same name overwrite each other and repeated runs
leave the same account behind more than once.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("utility_service_account_consolidation").getOrCreate()

# 2. Read every document in the drop with an explicit schema contract
service_account_landing_schema = StructType(fields=[
    StructField("accountNumber", IntegerType(), True),
    StructField("holderName", StringType(), True),
    StructField("holderAge", IntegerType(), True),
    StructField("streetAddress", StringType(), True),
    StructField("cityName", StringType(), True),
    StructField("regionName", StringType(), True),
    StructField("postalCode", StringType(), True),
])

service_account_landing_df = spark.read \
    .schema(service_account_landing_schema) \
    .json("data/landing/utility/service_account_records")

# 3. Publish every column under its business name
service_account_standardized_df = service_account_landing_df.select(
    col("accountNumber").alias("account_number"),
    col("holderName").alias("holder_name"),
    col("holderAge").alias("holder_age"),
    col("streetAddress").alias("street_address"),
    col("cityName").alias("city_name"),
    col("regionName").alias("region_name"),
    col("postalCode").alias("postal_code"))

# 4. Apply business rules: the account number is mandatory and unique
service_account_eligible_df = service_account_standardized_df \
    .filter(col("account_number").isNotNull()) \
    .dropDuplicates(["account_number"])

# 5. Gather the drop into a single file
service_account_df = service_account_eligible_df.coalesce(1)

# 6. Replace the consolidated dataset
service_account_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/utility/service_account")

# 7. Release resources
spark.stop()
