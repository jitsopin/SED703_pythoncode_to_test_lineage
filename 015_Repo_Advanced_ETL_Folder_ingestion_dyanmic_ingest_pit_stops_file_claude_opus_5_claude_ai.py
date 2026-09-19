"""
Business purpose: publish production line changeovers so downtime and shift
efficiency reporting share one record per changeover, tagged with the feed it
arrived from.
Source: data/landing/manufacturing/line_changeover_events.json, a full JSON extract
whose records span multiple lines and therefore require the multiLine option.
Target: warehouse/curated/manufacturing/line_changeover_detail, a Parquet snapshot
rebuilt on every run.
Output grain: one row per line_id, machine_id and changeover_sequence.
Business rules: the three key parts are mandatory, the changeover sequence arrives
as text and is stored as a whole number, every row records the feed that supplied
it, and each load carries one ingestion timestamp.
Generation profile: portable flat batch PySpark parameterized multiline-JSON event
ingestion.
Runtime arguments: argv[1] is the source system name written to source_system; it
falls back to "unspecified" when the argument is absent.
Assumptions: the extract is a complete snapshot, changeover sequences hold numeric
values only, and the load stamp is applied once because the original stamped the
same column twice in a row.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("manufacturing_line_changeover_ingestion").getOrCreate()
source_system = sys.argv[1] if len(sys.argv) > 1 else "unspecified"

# 2. Extract the multiline event snapshot with an explicit schema contract
changeover_landing_schema = StructType(fields=[
    StructField("lineId", IntegerType(), False),
    StructField("machineId", IntegerType(), True),
    StructField("changeoverSequence", StringType(), True),
    StructField("shiftSegment", IntegerType(), True),
    StructField("changeoverClock", StringType(), True),
    StructField("changeoverDuration", StringType(), True),
    StructField("durationMilliseconds", IntegerType(), True),
])

changeover_landing_df = spark.read \
    .schema(changeover_landing_schema) \
    .option("multiLine", True) \
    .json("data/landing/manufacturing/line_changeover_events.json")

# 3. Standardize business names and store the sequence as a whole number
changeover_standardized_df = changeover_landing_df.select(
    col("lineId").alias("line_id"),
    col("machineId").alias("machine_id"),
    col("changeoverSequence").cast(IntegerType()).alias("changeover_sequence"),
    col("shiftSegment").alias("shift_segment"),
    col("changeoverClock").alias("changeover_clock"),
    col("changeoverDuration").alias("changeover_duration"),
    col("durationMilliseconds").alias("duration_milliseconds"))

# 4. Apply business rules: every part of the business key is mandatory
changeover_eligible_df = changeover_standardized_df \
    .filter(col("line_id").isNotNull()
            & col("machine_id").isNotNull()
            & col("changeover_sequence").isNotNull())

# 5. Tag every row with the feed named by the runtime argument
changeover_tagged_df = changeover_eligible_df \
    .withColumn("source_system", lit(source_system))

# 6. Build the published projection with the load stamp
changeover_detail_df = changeover_tagged_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .select(col("line_id"),
            col("machine_id"),
            col("changeover_sequence"),
            col("shift_segment"),
            col("changeover_clock"),
            col("changeover_duration"),
            col("duration_milliseconds"),
            col("source_system"),
            col("ingestion_timestamp"))

# 7. Load the rebuilt snapshot into the curated layer
changeover_detail_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/manufacturing/line_changeover_detail")

# 8. Release resources
spark.stop()
