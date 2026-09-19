"""
Business purpose: publish the towing job exports in columnar form, keeping each
row traceable to the export that carried it and keeping the written files to a
workable size.
Source: data/landing/towing/job_exports, a folder of gzip compressed CSV exports,
one per month.
Target: warehouse/curated/towing/job_record, a Parquet dataset replaced on every
run and written in files of at most one hundred thousand rows.
Output grain: one row per towing job.
Business rules: the column types come from a declared contract rather than from
the sampled rows, every row records the export it came from, and no written file
holds more than one hundred thousand rows.
Generation profile: portable flat batch PySpark compressed CSV ingestion with
source tracking.
Runtime arguments: none.
Assumptions: the exports have already landed in the folder, since fetching them
over HTTP and uploading the result to object storage is transport rather than
transformation and has no portable equivalent; the month by month loop of the
original is expressed as reading the whole folder at once, which Spark handles
including the gzip; the hundred thousand row chunks it read and wrote are
expressed as a limit on the rows per written file; the export each row came from
is recorded because the original produced one file per export; and its checks for
work already done and its guard against an empty export were there to make a rerun
cheap and to stop early, not to decide what the output contains.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("towing_job_record_ingestion").getOrCreate()

# 2. Read every compressed export with an explicit schema contract
job_landing_schema = StructType(fields=[
    StructField("operatorId", IntegerType(), True),
    StructField("pickupTimestamp", TimestampType(), True),
    StructField("dropoffTimestamp", TimestampType(), True),
    StructField("offlineFlag", StringType(), True),
    StructField("rateCodeId", IntegerType(), True),
    StructField("pickupZoneId", IntegerType(), True),
    StructField("dropoffZoneId", IntegerType(), True),
    StructField("occupantCount", IntegerType(), True),
    StructField("jobDistanceMiles", DoubleType(), True),
    StructField("fareAmount", DoubleType(), True),
    StructField("extraCharge", DoubleType(), True),
    StructField("localTax", DoubleType(), True),
    StructField("tipAmount", DoubleType(), True),
    StructField("tollsAmount", DoubleType(), True),
    StructField("dispatchFee", DoubleType(), True),
    StructField("improvementSurcharge", DoubleType(), True),
    StructField("totalAmount", DoubleType(), True),
    StructField("congestionSurcharge", DoubleType(), True),
    StructField("paymentTypeId", IntegerType(), True),
    StructField("jobType", IntegerType(), True),
])

job_landing_df = spark.read \
    .option("header", True) \
    .schema(job_landing_schema) \
    .csv("data/landing/towing/job_exports")

# 3. Publish every column under its business name and record the export
job_record_df = job_landing_df.select(
    col("operatorId").alias("operator_id"),
    col("pickupTimestamp").alias("pickup_timestamp"),
    col("dropoffTimestamp").alias("dropoff_timestamp"),
    col("offlineFlag").alias("offline_flag"),
    col("rateCodeId").alias("rate_code_id"),
    col("pickupZoneId").alias("pickup_zone_id"),
    col("dropoffZoneId").alias("dropoff_zone_id"),
    col("occupantCount").alias("occupant_count"),
    col("jobDistanceMiles").alias("job_distance_miles"),
    col("fareAmount").alias("fare_amount"),
    col("extraCharge").alias("extra_charge"),
    col("localTax").alias("local_tax"),
    col("tipAmount").alias("tip_amount"),
    col("tollsAmount").alias("tolls_amount"),
    col("dispatchFee").alias("dispatch_fee"),
    col("improvementSurcharge").alias("improvement_surcharge"),
    col("totalAmount").alias("total_amount"),
    col("congestionSurcharge").alias("congestion_surcharge"),
    col("paymentTypeId").alias("payment_type_id"),
    col("jobType").alias("job_type"),
    input_file_name().alias("source_export"))

# 4. Replace the curated dataset, capped at a hundred thousand rows per file
job_record_df.write \
    .option("maxRecordsPerFile", 100000) \
    .mode("overwrite") \
    .parquet("warehouse/curated/towing/job_record")

# 5. Release resources
spark.stop()
