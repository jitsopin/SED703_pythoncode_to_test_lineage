"""
Business purpose: publish the performance schedule of each season so box office and
venue reporting share one dated listing, tagged with the feed it arrived from.
Source: data/landing/performing_arts/performance_listing.csv, a full CSV listing
with a header row.
Target: warehouse/curated/performing_arts/performance_schedule, a Parquet snapshot
partitioned by season_year and rebuilt on every run.
Output grain: one row per performance_id.
Business rules: performance_id and season_year are mandatory, the show date and
show time are combined into one scheduled start timestamp, every row records the
feed that supplied it, the public listing URL is not published, and each load
carries its own ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized schedule ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the listing is a complete snapshot delivered as one CSV file, show
times are recorded in the venue local zone as HH:mm:ss, and source_system is kept
in the published projection because the run is tagged for that purpose.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, to_timestamp
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("performing_arts_performance_schedule_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the listing with an explicit schema contract
performance_landing_schema = StructType(fields=[
    StructField("performanceId", IntegerType(), False),
    StructField("seasonYear", IntegerType(), True),
    StructField("runNumber", IntegerType(), True),
    StructField("venueId", IntegerType(), True),
    StructField("productionTitle", StringType(), True),
    StructField("showDate", DateType(), True),
    StructField("showTime", StringType(), True),
    StructField("listingUrl", StringType(), True),
])

performance_landing_df = spark.read \
    .option("header", True) \
    .schema(performance_landing_schema) \
    .csv("data/landing/performing_arts/performance_listing.csv")

# 3. Standardize business names and derive the scheduled start timestamp
performance_enriched_df = performance_landing_df.select(
    col("performanceId").alias("performance_id"),
    col("seasonYear").alias("season_year"),
    col("runNumber").alias("run_number"),
    col("venueId").alias("venue_id"),
    col("productionTitle").alias("production_title"),
    to_timestamp(concat_ws(" ", col("showDate"), col("showTime")),
                 "yyyy-MM-dd HH:mm:ss").alias("scheduled_start_ts"))

# 4. Apply business rules: mandatory key and partition column
performance_eligible_df = performance_enriched_df \
    .filter(col("performance_id").isNotNull() & col("season_year").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
performance_tagged_df = performance_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
performance_schedule_df = performance_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("performance_id"),
            col("season_year"),
            col("run_number"),
            col("venue_id"),
            col("production_title"),
            col("scheduled_start_ts"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the curated layer
performance_schedule_df.write \
    .mode("overwrite") \
    .partitionBy("season_year") \
    .parquet("warehouse/curated/performing_arts/performance_schedule")

# 8. Release resources
spark.stop()
