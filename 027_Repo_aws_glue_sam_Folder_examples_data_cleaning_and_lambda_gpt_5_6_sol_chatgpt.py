"""
Business purpose: Build a portable cleaned healthcare-payment dataset from a tabular source.
Source: Parquet healthcare payment data at data/landing/healthcare_payments/provider_payments.
Target: Curated Parquet dataset at data/curated/healthcare_payments/provider_payment_summary.
Output grain: One row per provider/payment record from the source after invalid provider identifiers are removed.
Important rules: Cast provider identifiers to long, remove rows with invalid provider identifiers, strip the leading currency symbol from charge/payment strings, cast those values to double, rename fields, build nested provider and charges structs, and write Parquet.
Generation profile: DATAFRAME_CHAIN + GLUE_CLEANING_PORT
Runtime arguments: None.
Assumptions: Currency fields contain a single leading currency symbol when non-null; source columns match the declared schema.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, struct
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_provider_payment_cleaning_pipeline").getOrCreate()

source_path = "data/landing/healthcare_payments/provider_payments"
target_path = "data/curated/healthcare_payments/provider_payment_summary"

payment_schema = StructType([
    StructField("drg_definition", StringType(), True),
    StructField("provider_id_raw", StringType(), True),
    StructField("provider_name", StringType(), True),
    StructField("provider_city", StringType(), True),
    StructField("provider_state", StringType(), True),
    StructField("provider_zip_code", LongType(), True),
    StructField("referral_region", StringType(), True),
    StructField("average_covered_charges_raw", StringType(), True),
    StructField("average_total_payments_raw", StringType(), True),
    StructField("average_medicare_payments_raw", StringType(), True),
])

# 2. Extract
payment_raw_df = (
    spark.read
    .schema(payment_schema)
    .parquet(source_path)
)

# 3. Resolve provider identifier type
provider_typed_df = payment_raw_df.select(
    col("drg_definition"),
    col("provider_id_raw").cast("long").alias("provider_id"),
    col("provider_name"),
    col("provider_city"),
    col("provider_state"),
    col("provider_zip_code"),
    col("referral_region"),
    col("average_covered_charges_raw"),
    col("average_total_payments_raw"),
    col("average_medicare_payments_raw"),
)

# 4. Remove records with invalid provider identifiers
provider_valid_df = provider_typed_df.filter(
    col("provider_id").isNotNull()
)

# 5. Clean and cast monetary values
payment_cleaned_df = provider_valid_df.select(
    col("drg_definition").alias("drg"),
    col("provider_id"),
    col("provider_name"),
    col("provider_city"),
    col("provider_state"),
    col("provider_zip_code"),
    col("referral_region").alias("rr"),
    regexp_replace(col("average_covered_charges_raw"), r"^.", "").cast(DoubleType()).alias("covered_charges"),
    regexp_replace(col("average_total_payments_raw"), r"^.", "").cast(DoubleType()).alias("total_payments"),
    regexp_replace(col("average_medicare_payments_raw"), r"^.", "").cast(DoubleType()).alias("medicare_payments"),
)

# 6. Build nested curated structure
published_payment_df = payment_cleaned_df.select(
    col("drg"),
    struct(
        col("provider_id").alias("id"),
        col("provider_name").alias("name"),
        col("provider_city").alias("city"),
        col("provider_state").alias("state"),
        col("provider_zip_code").alias("zip"),
    ).alias("provider"),
    col("rr"),
    struct(
        col("covered_charges").alias("covered"),
        col("total_payments").alias("total_pay"),
        col("medicare_payments").alias("medicare_pay"),
    ).alias("charges"),
)

# 7. Load
published_payment_df.write.mode("overwrite").parquet(target_path)

# 8. Release resources
spark.stop()
