"""
Business purpose: publish a clean insurance carrier registry so policy and claims
reporting resolve every contract to one carrier profile, tagged with the feed it
arrived from.
Source: data/landing/insurance/carrier_registry.json, a full JSON extract of the
carrier registry.
Target: warehouse/reference/insurance/carrier_master, a Parquet snapshot rebuilt
on every run.
Output grain: one row per carrier_id.
Business rules: carrier_id and carrier_code are mandatory, every row records the
feed that supplied it, the public website is not published, and each load carries
its own ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized registry ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the registry extract is a complete snapshot of line-delimited JSON
records.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("insurance_carrier_master_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the registry with an explicit DDL schema contract
carrier_landing_schema = ("carrierId INT, carrierCode STRING, carrierName STRING, "
                          "domicileCountry STRING, websiteUrl STRING")

carrier_landing_df = spark.read \
    .schema(carrier_landing_schema) \
    .json("data/landing/insurance/carrier_registry.json")

# 3. Standardize business names and drop the public website
carrier_standardized_df = carrier_landing_df.select(
    col("carrierId").alias("carrier_id"),
    col("carrierCode").alias("carrier_code"),
    col("carrierName").alias("carrier_name"),
    col("domicileCountry").alias("domicile_country"))

# 4. Apply business rules: mandatory key and carrier code
carrier_eligible_df = carrier_standardized_df \
    .filter(col("carrier_id").isNotNull() & col("carrier_code").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
carrier_tagged_df = carrier_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
carrier_master_df = carrier_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("carrier_id"),
            col("carrier_code"),
            col("carrier_name"),
            col("domicile_country"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the reference layer
carrier_master_df.write \
    .mode("overwrite") \
    .parquet("warehouse/reference/insurance/carrier_master")

# 8. Release resources
spark.stop()
