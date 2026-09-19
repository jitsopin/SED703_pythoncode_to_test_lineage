"""
Business purpose: publish the ferry crossing exports of one service line, split by
that line, so fare reporting reads each line's crossings on their own and the
optional charges are present whether or not the line records them.
Source: data/landing/ferry/crossing_exports, a folder of gzip compressed CSV
exports; the booking fee and the crossing type are only recorded by some lines and
arrive empty for the rest.
Target: warehouse/curated/ferry/crossing, a Parquet dataset partitioned by
service_line and replaced on every run.
Output grain: one row per crossing of the service line being loaded.
Business rules: the two optional charges are carried in the published set whether
or not the line records them, every row records the service line the run was asked
for, and the crossings are split by that line so a line can be read without
touching the others.
Generation profile: portable flat batch PySpark tagged ingestion into a
partitioned dataset.
Runtime arguments: argv[1] is the service line being loaded, written to
service_line and used to split the output; it falls back to "harbour".
Assumptions: the exports have already landed in the folder, since fetching them
over HTTP and uploading the result to object storage is transport rather than
transformation and has no portable equivalent; the month by month loop of the
original is expressed as reading the whole folder at once; the service it looped
over is taken as an argument because it named the object prefix rather than coming
from the data; and the optional columns are declared in the contract so a file
that omits them yields empty values instead of failing.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("ferry_crossing_line_ingestion").getOrCreate()
service_line = sys.argv[1] if len(sys.argv) > 1 else "harbour"

# 2. Read every export, declaring the optional charges so a file may omit them
crossing_landing_schema = StructType(fields=[
    StructField("operatorId", IntegerType(), True),
    StructField("boardingTime", TimestampType(), True),
    StructField("landingTime", TimestampType(), True),
    StructField("offlineFlag", StringType(), True),
    StructField("fareClassId", IntegerType(), True),
    StructField("departurePierId", IntegerType(), True),
    StructField("arrivalPierId", IntegerType(), True),
    StructField("passengerCount", IntegerType(), True),
    StructField("crossingDistanceNm", DoubleType(), True),
    StructField("fareAmount", DoubleType(), True),
    StructField("extraCharge", DoubleType(), True),
    StructField("harbourLevy", DoubleType(), True),
    StructField("tipAmount", DoubleType(), True),
    StructField("berthFee", DoubleType(), True),
    StructField("bookingFee", DoubleType(), True),
    StructField("terminalSurcharge", DoubleType(), True),
    StructField("totalAmount", DoubleType(), True),
    StructField("peakSurcharge", DoubleType(), True),
    StructField("paymentTypeId", IntegerType(), True),
    StructField("crossingTypeId", IntegerType(), True),
])

crossing_landing_df = spark.read \
    .option("header", True) \
    .schema(crossing_landing_schema) \
    .csv("data/landing/ferry/crossing_exports")

# 3. Publish every column under its business name
crossing_standardized_df = crossing_landing_df.select(
    col("operatorId").alias("operator_id"),
    col("boardingTime").alias("boarding_time"),
    col("landingTime").alias("landing_time"),
    col("offlineFlag").alias("offline_flag"),
    col("fareClassId").alias("fare_class_id"),
    col("departurePierId").alias("departure_pier_id"),
    col("arrivalPierId").alias("arrival_pier_id"),
    col("passengerCount").alias("passenger_count"),
    col("crossingDistanceNm").alias("crossing_distance_nm"),
    col("fareAmount").alias("fare_amount"),
    col("extraCharge").alias("extra_charge"),
    col("harbourLevy").alias("harbour_levy"),
    col("tipAmount").alias("tip_amount"),
    col("berthFee").alias("berth_fee"),
    col("bookingFee").alias("booking_fee"),
    col("terminalSurcharge").alias("terminal_surcharge"),
    col("totalAmount").alias("total_amount"),
    col("peakSurcharge").alias("peak_surcharge"),
    col("paymentTypeId").alias("payment_type_id"),
    col("crossingTypeId").alias("crossing_type_id"))

# 4. Record the service line the run was asked for
crossing_tagged_df = crossing_standardized_df \
    .withColumn("service_line", lit(service_line))

# 5. Replace the curated crossings, split by service line
crossing_tagged_df.write \
    .mode("overwrite") \
    .partitionBy("service_line") \
    .parquet("warehouse/curated/ferry/crossing")

# 6. Release resources
spark.stop()
