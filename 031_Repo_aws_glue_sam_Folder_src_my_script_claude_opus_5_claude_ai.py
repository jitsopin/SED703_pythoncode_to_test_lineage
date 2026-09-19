"""
Business purpose: two independent loads in one run, as in the original: keep a raw
copy of the loyalty point feed in columnar form, and publish the member directory
to the reporting database that the business intelligence tools read.
Sources: data/landing/loyalty/point_events, the CSV drop of point events, and the
catalogued table loyalty_curated.member_profile.
Targets: warehouse/raw/loyalty/point_events, a Parquet copy replaced on every run,
and the reporting database table reporting.member, appended over JDBC.
Output grain: one row per point event in the first load, and one row per member in
the second.
Business rules: the point feed is copied without change, the member identifier is
published as a whole number, and only the identifier and the two name parts are
published to the reporting database.
Generation profile: portable flat batch PySpark copy and database publish.
Runtime arguments: argv[1] is the JDBC url of the reporting database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: the catalogued table already exists; credentials are taken from the
arguments instead of being written into the script as the original did; and the
deletion of the target prefix before writing is expressed as an overwrite, which
leaves the same result.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("loyalty_point_and_member_publish").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/reporting"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the point event drop with an explicit schema contract
point_event_landing_schema = StructType(fields=[
    StructField("eventId", StringType(), False),
    StructField("memberId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("pointAmount", DoubleType(), True),
    StructField("eventDate", DateType(), True),
])

point_event_landing_df = spark.read \
    .option("header", True) \
    .schema(point_event_landing_schema) \
    .csv("data/landing/loyalty/point_events")

# 3. Replace the raw copy with this run's feed
point_event_landing_df.write \
    .mode("overwrite") \
    .parquet("warehouse/raw/loyalty/point_events")

# 4. Read the member directory, naming the columns it supplies
member_profile_landing_df = spark.table("loyalty_curated.member_profile").select(
    col("memberId"),
    col("givenName"),
    col("familyName"))

# 5. Publish the identifier as a whole number under business names
member_profile_df = member_profile_landing_df.select(
    col("memberId").cast(IntegerType()).alias("member_id"),
    col("givenName").alias("given_name"),
    col("familyName").alias("family_name"))

# 6. Append the member directory to the reporting database
member_profile_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "reporting.member") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("append") \
    .save()

# 7. Release resources
spark.stop()
