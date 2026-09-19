"""Business purpose: Ingest flight manifests and synchronize airline loyalty members.
Source 1: data/landing/airlines/flight_manifests.csv
Target 1: data/curated/airlines/manifests_processed (Parquet format)
Source 2: airline_catalog.loyalty_members (Spark catalog table)
Target 2: Operational PostgreSQL Database (JDBC)
Output grain: Dual pipeline processing flight logs and individual loyalty profiles.
Business rules: Archive raw manifests as Parquet, and cast member identifiers before syncing to the operational database.
Generation profile: Flat portable PySpark script; AWS Glue contexts, dynamic frames, and ApplyMapping removed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("AirlineManifestAndLoyaltyETL").getOrCreate()

# ==========================================
# Pipeline Track 1: Raw Manifest Archival
# ==========================================

# 2. Extract raw flight manifests from landing zone
manifest_raw_df = spark.read.option("header", "true").csv("data/landing/airlines/flight_manifests.csv")

# 3. Load manifests to curated storage (Overwrite mode replaces proprietary directory purge logic)
manifest_raw_df.write.mode("overwrite").parquet("data/curated/airlines/manifests_processed")


# ==========================================
# Pipeline Track 2: Loyalty Member Sync
# ==========================================

# 4. Extract loyalty member data from the data catalog
loyalty_raw_df = spark.table("airline_catalog.loyalty_members")

# 5. Apply type mapping and standardization natively
loyalty_mapped_df = loyalty_raw_df.select(
    col("member_id_str").cast(IntegerType()).alias("member_id"),
    col("first_name").cast(StringType()).alias("first_name"),
    col("last_name").cast(StringType()).alias("last_name")
)

# 6. Synchronize mapped loyalty profiles to the operational PostgreSQL database
jdbc_url = "jdbc:postgresql://db-airline-prod.internal:5432/operational_db"
jdbc_properties = {
        "user": "admin_user",
        "password": "secure_password",
        "driver": "org.postgresql.Driver"
}

loyalty_mapped_df.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="public.loyalty_profiles",
    properties=jdbc_properties
)

# 7. Release resources
spark.stop()