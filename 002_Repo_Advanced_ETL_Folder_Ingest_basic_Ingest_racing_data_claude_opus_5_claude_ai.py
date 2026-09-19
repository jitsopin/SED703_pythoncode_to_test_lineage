"""
Business purpose: publish the scheduled port calls of the shipping network so
berth planning and arrival reporting share one dated schedule.
Source: data/landing/shipping/port_call_extract.csv, a full CSV extract of the
published schedule with a header row.
Target: warehouse/curated/shipping/port_call_schedule, a Parquet snapshot
partitioned by schedule_year and rebuilt on every run.
Output grain: one row per port_call_id.
Business rules: port_call_id and schedule_year are mandatory, the call date and
call time are combined into a single scheduled departure timestamp with midnight
assumed when the time is missing, the public schedule URL is not published, and
each load carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark scheduled-event ingestion.
Runtime arguments: none.
Assumptions: the landing extract is a complete snapshot delivered as one CSV file
and call times are recorded in the port local zone as HH:mm:ss.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, lit, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("shipping_port_call_schedule_ingestion").getOrCreate()

# 2. Extract the landing snapshot with an explicit schema contract
port_call_landing_schema = StructType(fields=[
    StructField("portCallId", IntegerType(), False),
    StructField("scheduleYear", IntegerType(), True),
    StructField("voyageLeg", IntegerType(), True),
    StructField("terminalId", IntegerType(), True),
    StructField("portCallName", StringType(), True),
    StructField("callDate", DateType(), True),
    StructField("callTime", StringType(), True),
    StructField("scheduleUrl", StringType(), True),
])

port_call_landing_df = spark.read \
    .option("header", True) \
    .schema(port_call_landing_schema) \
    .csv("data/landing/shipping/port_call_extract.csv")

# 3. Standardize business names and derive the scheduled departure timestamp
port_call_enriched_df = port_call_landing_df.select(
    col("portCallId").alias("port_call_id"),
    col("scheduleYear").alias("schedule_year"),
    col("voyageLeg").alias("voyage_leg"),
    col("terminalId").alias("terminal_id"),
    col("portCallName").alias("port_call_name"),
    to_timestamp(concat_ws(" ", col("callDate"), coalesce(col("callTime"), lit("00:00:00"))),
                 "yyyy-MM-dd HH:mm:ss").alias("scheduled_departure_ts"))

# 4. Apply business rules: mandatory key and partition column
port_call_eligible_df = port_call_enriched_df \
    .filter(col("port_call_id").isNotNull() & col("schedule_year").isNotNull())

# 5. Build the published projection with the load stamp
port_call_schedule_df = port_call_eligible_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("port_call_id"),
            col("schedule_year"),
            col("voyage_leg"),
            col("terminal_id"),
            col("port_call_name"),
            col("scheduled_departure_ts"),
            col("ingestion_timestamp"))

# 6. Load the rebuilt snapshot into the curated layer
port_call_schedule_df.write \
    .mode("overwrite") \
    .partitionBy("schedule_year") \
    .parquet("warehouse/curated/shipping/port_call_schedule")

# 7. Release resources
spark.stop()
