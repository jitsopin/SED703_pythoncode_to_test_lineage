"""
Business purpose: publish the play log of every advertising slot so airtime
billing and campaign delivery reporting share one record per slot, tagged with the
feed it arrived from.
Source: data/landing/broadcast/ad_play_logs, a folder of headerless CSV parts that
Spark reads as one dataset.
Target: the managed table broadcast_curated.ad_play_detail, stored as Parquet and
replaced on every run.
Output grain: one row per campaign_id, channel_id and break_index.
Business rules: the three key parts are mandatory, column positions come from the
declared schema because the CSV parts carry no header row, every row records the
feed that supplied it, and each load carries one ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized multi-part CSV
folder ingestion into a managed table.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: every CSV part shares the same column order, the target database
already exists, and the load stamp is applied once because the original stamped
the same column twice in a row.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("broadcast_ad_play_detail_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract every CSV part of the landing folder with an explicit schema contract
ad_play_landing_schema = StructType(fields=[
    StructField("campaignId", IntegerType(), False),
    StructField("channelId", IntegerType(), True),
    StructField("breakIndex", IntegerType(), True),
    StructField("slotOrder", IntegerType(), True),
    StructField("airClock", StringType(), True),
    StructField("airDurationMilliseconds", IntegerType(), True),
])

ad_play_landing_df = spark.read \
    .option("header", False) \
    .schema(ad_play_landing_schema) \
    .csv("data/landing/broadcast/ad_play_logs")

# 3. Standardize business names for the curated layer
ad_play_standardized_df = ad_play_landing_df.select(
    col("campaignId").alias("campaign_id"),
    col("channelId").alias("channel_id"),
    col("breakIndex").alias("break_index"),
    col("slotOrder").alias("slot_order"),
    col("airClock").alias("air_clock"),
    col("airDurationMilliseconds").alias("air_duration_milliseconds"))

# 4. Apply business rules: every part of the business key is mandatory
ad_play_eligible_df = ad_play_standardized_df \
    .filter(col("campaign_id").isNotNull()
            & col("channel_id").isNotNull()
            & col("break_index").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
ad_play_tagged_df = ad_play_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
ad_play_detail_df = ad_play_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("campaign_id"),
            col("channel_id"),
            col("break_index"),
            col("slot_order"),
            col("air_clock"),
            col("air_duration_milliseconds"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Replace the managed table with the rebuilt snapshot
ad_play_detail_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("broadcast_curated.ad_play_detail")

# 8. Release resources
spark.stop()
