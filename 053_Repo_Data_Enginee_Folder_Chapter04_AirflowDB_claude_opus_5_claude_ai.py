"""
Business purpose: feed the attendee search index from the operational database, by
way of an extract file that keeps the two halves of the job independent.
Source: the table events_db.attendee in the operational database, read over JDBC
for the two attributes the index holds.
Targets: data/staging/events/attendee_extract, the extract written as CSV, and
warehouse/search/events/attendee_index, the search feed written as one JSON
document per attendee; both are replaced on every run.
Output grain: one row per attendee in both targets.
Business rules: only the attendee name and the city are published, and the search
feed is built from the extract rather than from the database, so the index can be
rebuilt without querying the operational system again.
Generation profile: portable flat batch PySpark extract and search feed build.
Runtime arguments: argv[1] is the JDBC url of the operational database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: the schedule and the task wiring of the original belong to its
orchestrator and say nothing about the data, so they are not part of this
pipeline; the search index is written as JSON documents to a dataset because
indexing them one call at a time has no portable equivalent, and one document per
row is what the original sent; credentials are taken from the arguments rather
than written into the script; and the extract carries no row number, which the
original added only because its writer numbers rows by default.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("events_attendee_search_feed").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/operations"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Pull the two published attributes out of the operational table
attendee_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", "SELECT attendee_name, city_name FROM events_db.attendee") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load()

# 3. Write the extract the second half of the job reads
attendee_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .save("data/staging/events/attendee_extract")

# 4. Read the extract back with an explicit schema contract
attendee_extract_schema = StructType(fields=[
    StructField("attendee_name", StringType(), True),
    StructField("city_name", StringType(), True),
])

attendee_extract_df = spark.read \
    .option("header", True) \
    .schema(attendee_extract_schema) \
    .csv("data/staging/events/attendee_extract")

# 5. Build the search feed, one document per attendee
attendee_index_df = attendee_extract_df.select(
    col("attendee_name"),
    col("city_name"))

# 6. Replace the search feed
attendee_index_df.write \
    .format("json") \
    .mode("overwrite") \
    .save("warehouse/search/events/attendee_index")

# 7. Release resources
spark.stop()
