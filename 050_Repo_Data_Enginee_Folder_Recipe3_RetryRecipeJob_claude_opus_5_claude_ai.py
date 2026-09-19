"""
Business purpose: separate the reservations that pass validation from the ones
that do not, so the curated feed holds only usable rows while every rejected row
is kept with the reason it was held back.
Source: data/landing/hospitality/reservation_feed.csv, the reservation drop with a
header row.
Targets: warehouse/curated/hospitality/reservation for the rows that pass, and
warehouse/quarantine/hospitality/reservation_rejected for the rows that do not,
both replaced on every run.
Output grain: one row per reservation in whichever of the two targets it lands in.
Business rules: a reservation must carry an identifier, must check out after it
checks in, must carry a room rate above zero and must be for at least one guest;
the first rule a reservation breaks is the reason recorded against it, and a
rejected row also records when it was held back.
Generation profile: portable flat batch PySpark validation with a rejected output.
Runtime arguments: none.
Assumptions: the source file exists to demonstrate how a job behaves when it is
retried, by dividing by zero on its first attempt and skipping that on the second,
so it holds no data flow and this pipeline is not extracted from it; the datasets,
the columns and the rules above are constructed around its subject of handling a
failure, expressed here as holding a bad row back rather than failing the run, and
none of them appear in the original.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("hospitality_reservation_validation").getOrCreate()

# 2. Read the reservation drop with an explicit schema contract
reservation_landing_schema = StructType(fields=[
    StructField("reservationId", StringType(), True),
    StructField("guestName", StringType(), True),
    StructField("propertyCode", StringType(), True),
    StructField("checkInDate", DateType(), True),
    StructField("checkOutDate", DateType(), True),
    StructField("roomRate", DoubleType(), True),
    StructField("guestCount", IntegerType(), True),
])

reservation_landing_df = spark.read \
    .option("header", True) \
    .schema(reservation_landing_schema) \
    .csv("data/landing/hospitality/reservation_feed.csv")

# 3. Publish every column under its business name
reservation_standardized_df = reservation_landing_df.select(
    col("reservationId").alias("reservation_id"),
    col("guestName").alias("guest_name"),
    col("propertyCode").alias("property_code"),
    col("checkInDate").alias("check_in_date"),
    col("checkOutDate").alias("check_out_date"),
    col("roomRate").alias("room_rate"),
    col("guestCount").alias("guest_count"))

# 4. Record the first rule each reservation breaks, if any
reservation_checked_df = reservation_standardized_df.withColumn(
    "rejection_reason",
    when(col("reservation_id").isNull(), lit("missing reservation id"))
    .when(col("check_out_date") <= col("check_in_date"), lit("check out not after check in"))
    .when(col("room_rate") <= 0, lit("room rate not above zero"))
    .when(col("guest_count") < 1, lit("guest count below one"))
    .otherwise(lit(None).cast("string")))

# 5. Keep the reservations that broke no rule
reservation_valid_df = reservation_checked_df \
    .filter(col("rejection_reason").isNull()) \
    .select(col("reservation_id"),
            col("guest_name"),
            col("property_code"),
            col("check_in_date"),
            col("check_out_date"),
            col("room_rate"),
            col("guest_count"))

# 6. Publish the curated reservations
reservation_valid_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/hospitality/reservation")

# 7. Hold back the reservations that broke a rule, with the reason and the moment
reservation_rejected_df = reservation_checked_df \
    .filter(col("rejection_reason").isNotNull()) \
    .withColumn("rejected_timestamp", current_timestamp())

# 8. Publish the held back reservations
reservation_rejected_df.write \
    .mode("overwrite") \
    .parquet("warehouse/quarantine/hospitality/reservation_rejected")

# 9. Release resources
spark.stop()
