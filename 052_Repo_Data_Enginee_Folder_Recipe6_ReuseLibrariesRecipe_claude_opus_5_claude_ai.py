"""
Business purpose: price every waste collection so billing reads the charge instead
of applying the area tariff itself.
Sources: data/curated/waste/collection_record, one row per collection, and the
table reference.waste_tariff, which holds the rate each service area is charged
at.
Target: warehouse/curated/waste/collection_charge, a Parquet dataset replaced on
every run.
Output grain: one row per collection_id.
Business rules: a collection is priced only when its service area has a tariff, so
the join is an inner join, and the charge is the weight collected times the area
rate rounded to two decimals.
Generation profile: portable flat batch PySpark rate lookup and charge
calculation.
Runtime arguments: none.
Assumptions: the source file exists to show how a shared module is imported, so
its two calls stand in for a configuration value and a calculation without saying
what either of them is, and it reads no data at all; the datasets, the columns and
the rate rule above are constructed around that subject, with the configuration
value expressed as a rate looked up per service area so the charge can be traced
back to the row that set it; and none of them appear in the original.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

# 1. Initialize Spark
spark = SparkSession.builder.appName("waste_collection_charge_pricing").getOrCreate()

# 2. Read the collections to be priced
collection_record_df = spark.read.parquet("data/curated/waste/collection_record").select(
    col("collection_id"),
    col("customer_id"),
    col("service_area_code"),
    col("collected_kg"),
    col("collected_at"))

# 3. Read the rate each service area is charged at
waste_tariff_df = spark.table("reference.waste_tariff").select(
    col("service_area_code"),
    col("rate_per_kg"))

# 4. Attach the area rate to every collection
collection_rated_df = collection_record_df.join(
    waste_tariff_df, ["service_area_code"], "inner")

# 5. Price the collection at the area rate
collection_charge_df = collection_rated_df.withColumn(
    "charge_amount",
    spark_round(col("collected_kg") * col("rate_per_kg"), 2))

# 6. Replace the priced collections
collection_charge_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/waste/collection_charge")

# 7. Release resources
spark.stop()
