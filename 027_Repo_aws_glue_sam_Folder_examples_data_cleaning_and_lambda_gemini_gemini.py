"""Business purpose: Clean, standardize, and nest real estate property listings.
Source: data/landing/real_estate/raw_listings.csv
Target: data/curated/real_estate/nested_listings (Parquet format)
Output grain: One record per real estate property listing.
Business rules: Drop invalid broker IDs, strip currency symbols, cast price types, and nest broker/pricing details.
Generation profile: Native PySpark standard APIs replacing proprietary Glue dynamic frames and UDFs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, struct
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("RealEstateListingsETL").getOrCreate()

# 2. Extract raw real estate listings
listing_schema = StructType([
    StructField("property_desc", StringType(), True),
    StructField("broker_id_raw", StringType(), True),
    StructField("broker_name", StringType(), True),
    StructField("broker_city", StringType(), True),
    StructField("broker_state", StringType(), True),
    StructField("broker_zip", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("asking_price_raw", StringType(), True),
    StructField("closing_price_raw", StringType(), True),
    StructField("tax_assessment_raw", StringType(), True)
])
listings_raw_df = spark.read.option("header", "true").schema(listing_schema).csv("data/landing/real_estate/raw_listings.csv")

# 3. Cast identifying keys and remove invalid records
listings_filtered_df = listings_raw_df.withColumn("broker_id", col("broker_id_raw").cast(LongType())) \
    .filter(col("broker_id").isNotNull())

# 4. Remove currency symbols from monetary strings using native expressions
listings_cleaned_df = listings_filtered_df.withColumn("clean_asking", expr("substring(asking_price_raw, 2, length(asking_price_raw))")) \
    .withColumn("clean_closing", expr("substring(closing_price_raw, 2, length(closing_price_raw))")) \
    .withColumn("clean_tax", expr("substring(tax_assessment_raw, 2, length(tax_assessment_raw))"))

# 5. Rename, cast, and group into nested structures
listings_nested_df = listings_cleaned_df.select(
    col("property_desc").alias("property_type"),
    col("region_code").alias("operating_region"),
    struct(
        col("broker_id").alias("id"),
        col("broker_name").alias("name"),
        col("broker_city").alias("city"),
        col("broker_state").alias("state"),
        col("broker_zip").cast(LongType()).alias("zip")
    ).alias("broker"),
    struct(
        col("clean_asking").cast(DoubleType()).alias("asking_price"),
        col("clean_closing").cast(DoubleType()).alias("closing_price"),
        col("clean_tax").cast(DoubleType()).alias("tax_assessment")
    ).alias("pricing")
)

# 6. Load curated nested listings
listings_nested_df.write.mode("overwrite").parquet("data/curated/real_estate/nested_listings")

# 7. Release resources
spark.stop()