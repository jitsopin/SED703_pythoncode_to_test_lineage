"""
Business purpose: move the car park session drops into the curated area a few
files at a time, so a run picks up only what has arrived since the previous one
and leaves one file behind per run.
Source: data/landing/parking/session_events, a folder that receives JSON drops of
completed parking sessions.
Target: warehouse/curated/parking/session_events, written as JSON and added to on
every run.
Output grain: one row per parking session.
Business rules: the sessions are copied without change, a run takes at most five
drops so a backlog is worked through in steady pieces, and a drop already taken by
an earlier run is never taken again.
Generation profile: portable PySpark incremental file ingestion.
Runtime arguments: none.
Assumptions: the bookmark and the file limit of the original are expressed as a
checkpointed stream that reads at most five files per trigger and runs until it
has caught up, which is the closest portable equivalent of remembering what has
been processed; the run stops once the backlog is cleared, matching a job that is
scheduled rather than left running; and the column set is declared here because
the original read the drops without stating one.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, StringType, StructField, StructType,
                               TimestampType)

# 1. Initialize Spark
spark = SparkSession.builder.appName("parking_session_event_ingestion").getOrCreate()

# 2. Take at most five new drops, with an explicit schema contract
session_event_schema = StructType(fields=[
    StructField("session_id", StringType(), True),
    StructField("car_park_code", StringType(), True),
    StructField("entry_time", TimestampType(), True),
    StructField("exit_time", TimestampType(), True),
    StructField("vehicle_class", StringType(), True),
    StructField("amount_paid", DoubleType(), True),
])

session_event_df = spark.readStream \
    .schema(session_event_schema) \
    .option("maxFilesPerTrigger", 5) \
    .json("data/landing/parking/session_events")

# 3. Leave one file behind per run
session_event_single_df = session_event_df.coalesce(1)

# 4. Add the drops to the curated area
session_event_query = session_event_single_df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("checkpointLocation", "warehouse/checkpoints/parking_session_events") \
    .option("path", "warehouse/curated/parking/session_events") \
    .trigger(availableNow=True) \
    .start()

# 5. Run until the backlog is cleared
session_event_query.awaitTermination()

# 6. Release resources
spark.stop()
