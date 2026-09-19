"""
Business purpose: publish the charging stops of every fleet route so energy and
turnaround reporting share one record per stop.
Source: data/landing/fleet/charging_stop_events.json, a full JSON extract whose
records span multiple lines and therefore require the multiLine reader option.
Target: warehouse/curated/fleet/charging_stop_detail, a Parquet snapshot rebuilt
on every run.
Output grain: one row per route_id, vehicle_id and stop_sequence.
Business rules: the three key parts are mandatory, the stop sequence arrives as
text and is stored as a whole number, and each load carries its own ingestion
timestamp.
Generation profile: portable flat batch PySpark multiline-JSON event ingestion.
Runtime arguments: none.
Assumptions: the event extract is a complete snapshot and stop sequences hold
numeric values only.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("fleet_charging_stop_detail_ingestion").getOrCreate()

# 2. Extract the multiline event snapshot with an explicit schema contract
charging_stop_landing_schema = StructType(fields=[
    StructField("routeId", IntegerType(), False),
    StructField("vehicleId", IntegerType(), True),
    StructField("stopSequence", StringType(), True),
    StructField("segmentIndex", IntegerType(), True),
    StructField("stopClock", StringType(), True),
    StructField("chargeDuration", StringType(), True),
    StructField("durationMilliseconds", IntegerType(), True),
])

charging_stop_landing_df = spark.read \
    .schema(charging_stop_landing_schema) \
    .option("multiLine", True) \
    .json("data/landing/fleet/charging_stop_events.json")

# 3. Standardize business names and store the stop sequence as a whole number
charging_stop_standardized_df = charging_stop_landing_df.select(
    col("routeId").alias("route_id"),
    col("vehicleId").alias("vehicle_id"),
    col("stopSequence").cast(IntegerType()).alias("stop_sequence"),
    col("segmentIndex").alias("segment_index"),
    col("stopClock").alias("stop_clock"),
    col("chargeDuration").alias("charge_duration"),
    col("durationMilliseconds").alias("duration_milliseconds"))

# 4. Apply business rules: every part of the business key is mandatory
charging_stop_eligible_df = charging_stop_standardized_df \
    .filter(col("route_id").isNotNull()
            & col("vehicle_id").isNotNull()
            & col("stop_sequence").isNotNull())

# 5. Build the published projection with the load stamp
charging_stop_detail_df = charging_stop_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("route_id"),
            col("vehicle_id"),
            col("stop_sequence"),
            col("segment_index"),
            col("stop_clock"),
            col("charge_duration"),
            col("duration_milliseconds"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the curated layer
charging_stop_detail_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/fleet/charging_stop_detail")

# 7. Release resources
spark.stop()
