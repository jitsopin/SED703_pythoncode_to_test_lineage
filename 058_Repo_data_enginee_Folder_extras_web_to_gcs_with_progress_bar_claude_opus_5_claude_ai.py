"""
Business purpose: publish the towing job records in columnar form and split by
month, so billing and utilisation reporting read typed columns without unpacking
the compressed drops.
Source: data/landing/towing/job_records, a folder of gzip compressed CSV drops,
one per month, each with a header row.
Target: warehouse/curated/towing/job_record, a Parquet dataset partitioned by
pickup_month and replaced on every run.
Output grain: one row per towing job.
Business rules: the column types come from a declared contract rather than from
the sampled rows, a job without a pickup time is not published because the
partition is derived from it, and the pickup month is written as yyyy-MM.
Generation profile: portable flat batch PySpark compressed CSV ingestion.
Runtime arguments: none.
Assumptions: the drops have already landed in the folder, since fetching them over
HTTP and uploading the result to object storage is transport rather than
transformation and has no portable equivalent; the month by month loop of the
original is expressed as reading the whole folder at once, which Spark handles
including the gzip; the one file per month layout it produced is expressed as a
partition on the pickup month; and its checks for work already done were there to
make a rerun cheap, not to decide what the output contains.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("towing_job_record_ingestion").getOrCreate()

# 2. Read every compressed drop with an explicit schema contract
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
    .csv("data/landing/towing/job_records")

# 3. Publish every column under its business name
job_standardized_df = job_landing_df.select(
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
    col("jobType").alias("job_type"))

# 4. Apply business rules: the pickup time carries the partition
job_eligible_df = job_standardized_df \
    .filter(col("pickup_timestamp").isNotNull())

# 5. Derive the month the job was picked up in
job_record_df = job_eligible_df \
    .withColumn("pickup_month", date_format(col("pickup_timestamp"), "yyyy-MM"))

# 6. Replace the curated dataset, split by month
job_record_df.write \
    .mode("overwrite") \
    .partitionBy("pickup_month") \
    .parquet("warehouse/curated/towing/job_record")

# 7. Release resources
spark.stop()
