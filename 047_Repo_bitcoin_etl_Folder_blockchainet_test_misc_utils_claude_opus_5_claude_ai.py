"""
Business purpose: publish only the ticket events that arrived through one channel,
so each channel team reads a feed containing its own traffic and nothing else.
Source: data/landing/support/ticket_events, a folder of JSON documents, one ticket
event per line.
Target: warehouse/curated/support/channel_ticket_events, written as one JSON
document per line and replaced on every run.
Output grain: one row per ticket event of the requested channel.
Business rules: an event is published only when its channel matches the channel
the run was asked for, and every column is published under its business name.
Generation profile: portable flat batch PySpark record filter.
Runtime arguments: argv[1] is the channel to keep; it falls back to "email".
Assumptions: the source file is a unit test that writes two records to a temporary
file and checks what comes back, so it holds no datasets of its own and this
pipeline is not extracted from it; the rule above is the predicate that test
supplied, an equality check on a single field, and the channel is taken as an
argument because that test passed the predicate in as a parameter; the datasets
and the column set are named here and none of them appear in the original.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("support_channel_ticket_events").getOrCreate()
kept_channel = sys.argv[1] if len(sys.argv) > 1 else "email"

# 2. Read the ticket event feed with an explicit schema contract
ticket_event_schema = StructType(fields=[
    StructField("ticketId", StringType(), True),
    StructField("channelCode", StringType(), True),
    StructField("queueName", StringType(), True),
    StructField("priorityCode", StringType(), True),
    StructField("subjectLine", StringType(), True),
    StructField("openedAt", TimestampType(), True),
])

ticket_event_landing_df = spark.read \
    .schema(ticket_event_schema) \
    .json("data/landing/support/ticket_events")

# 3. Publish every column under its business name
ticket_event_standardized_df = ticket_event_landing_df.select(
    col("ticketId").alias("ticket_id"),
    col("channelCode").alias("channel_code"),
    col("queueName").alias("queue_name"),
    col("priorityCode").alias("priority_code"),
    col("subjectLine").alias("subject_line"),
    col("openedAt").alias("opened_at"))

# 4. Keep only the events of the requested channel
channel_ticket_event_df = ticket_event_standardized_df \
    .filter(col("channel_code") == kept_channel)

# 5. Replace the curated feed
channel_ticket_event_df.write \
    .format("json") \
    .mode("overwrite") \
    .save("warehouse/curated/support/channel_ticket_events")

# 6. Release resources
spark.stop()
