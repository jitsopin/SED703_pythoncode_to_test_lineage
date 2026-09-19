"""Business purpose: Ingest, type-cast, and convert NYC TLC taxi CSV data to Parquet format on Google Cloud Storage.
Source: gs://dtc-data-lake-bucketname/landing/green/ (CSV format)
Target: gs://dtc-data-lake-bucketname/green/ (Parquet format)
Output grain: One record per taxi trip.
Business rules: Extract raw CSVs from the GCS landing zone, strictly enforce numerical and timestamp data types, and persist to the curated GCS bucket.
Generation profile: Portable PySpark script replacing local Pandas iteration, HTTP requests, and the GCS Python SDK.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("NYCTaxiGCS_Ingestion").getOrCreate()
bucket_name = "dtc-data-lake-bucketname"

# 2. Define strict schema mirroring the original pandas dtypes
green_taxi_schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehailfee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("trip_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# 3. Extract all available month files simultaneously via wildcard (Replacing iterative downloads)
taxis_raw_df = spark.read \
    .option("header", "true") \
    .schema(green_taxi_schema) \
    .csv(f"gs://{bucket_name}/landing/green/green_tripdata_*.csv.gz")

# 4. Load the standardized dataframe directly to GCS in Parquet format (Replacing local staging and google-cloud-storage sdk)
taxis_raw_df.write \
    .mode("overwrite") \
    .parquet(f"gs://{bucket_name}/green/")

# 5. Release resources
spark.stop()