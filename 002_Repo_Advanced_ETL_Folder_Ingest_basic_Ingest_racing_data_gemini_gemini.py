"""Business purpose: Ingest, enrich, and partition cycling stage events.
Source: data/landing/cycling/stage_events.csv
Target: data/curated/cycling/stage_events_partitioned
Output grain: One record per cycling stage.
Business rules: Append ingestion time, combine event date/time to timestamp, rename columns, partition by season.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("CyclingStageIngestion").getOrCreate()

# 2. Define schema and extract stage events
stage_schema = StructType([
    StructField("stage_id", IntegerType(), False),
    StructField("season", IntegerType(), True),
    StructField("stage_number", IntegerType(), True),
    StructField("route_id", IntegerType(), True),
    StructField("stage_name", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("start_time", StringType(), True),
    StructField("live_tracker_url", StringType(), True)
])
stage_raw_df = spark.read.option("header", "true").schema(stage_schema).csv("data/landing/cycling/stage_events.csv")

# 3. Add derived timestamps for event start and processing time
stage_enriched_df = stage_raw_df.withColumn("processed_timestamp", current_timestamp()) \
    .withColumn("start_datetime", to_timestamp(concat(col("event_date"), lit(" "), col("start_time")), "yyyy-MM-dd HH:mm:ss"))

# 4. Select relevant columns and standardize naming
stage_final_df = stage_enriched_df.select(
    col("stage_id").alias("event_id"),
    col("season").alias("event_season"),
    col("stage_number"),
    col("route_id"),
    col("stage_name"),
    col("processed_timestamp"),
    col("start_datetime")
)

# 5. Persist curated stage records partitioned by season
stage_final_df.write.mode("overwrite").partitionBy("event_season").parquet("data/curated/cycling/stage_events_partitioned")

# 6. Release resources
spark.stop()