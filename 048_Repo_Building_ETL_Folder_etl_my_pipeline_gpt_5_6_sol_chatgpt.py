"""
Business purpose: Build crash, vehicle, and person analytical datasets from three traffic-safety sources and append them to relational targets.
Sources: CSV datasets at data/landing/traffic/crashes.csv, data/landing/traffic/vehicles.csv, and data/landing/traffic/people.csv.
Targets: JDBC tables analytics.crash, analytics.vehicle, and analytics.person.
Output grain: crash output contains one row per joined crash/person/vehicle combination projected to crash fields; vehicle output contains one row per joined combination projected to vehicle fields; person output contains one row per joined combination projected to person fields.
Important rules: Left join crashes to vehicles on CRASH_RECORD_ID, then left join the result to people on CRASH_RECORD_ID; project exactly the business columns visible in the source pipeline; append each result to its corresponding relational target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (THREE_BRANCH_RELATIONAL_LOAD)
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: The source file omits the concrete vehicle/people CSV read statements and truncates the Bonobo graph definition, but clearly establishes the three logical inputs, join keys, output projections, and append-style database loads; those visible semantics are preserved without inventing additional business rules.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

spark = SparkSession.builder.appName("portable_traffic_crash_pipeline").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/analytics"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "analytics_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

crashes_path = "data/landing/traffic/crashes.csv"
vehicles_path = "data/landing/traffic/vehicles.csv"
people_path = "data/landing/traffic/people.csv"

crash_target = "analytics.crash"
vehicle_target = "analytics.vehicle"
person_target = "analytics.person"

crashes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(crashes_path)
)

vehicles_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(vehicles_path)
)

people_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(people_path)
)

crash_vehicles_df = crashes_df.alias("c").join(
    vehicles_df.alias("v"),
    col("c.CRASH_RECORD_ID") == col("v.CRASH_RECORD_ID"),
    "left",
)

merged_all_df = crash_vehicles_df.alias("cv").join(
    people_df.alias("p"),
    col("cv.CRASH_RECORD_ID") == col("p.CRASH_RECORD_ID"),
    "left",
)

crash_df = merged_all_df.select(
    col("cv.CRASH_UNIT_ID").alias("CRASH_UNIT_ID_x"),
    col("cv.CRASH_RECORD_ID").alias("CRASH_RECORD_ID"),
    col("cv.CRASH_DATE").alias("CRASH_DATE_x"),
    col("p.PERSON_ID").alias("PERSON_ID"),
    col("cv.VEHICLE_ID").alias("VEHICLE_ID"),
    col("cv.NUM_UNITS").alias("NUM_UNITS"),
    col("cv.TOTAL_INJURIES").alias("TOTAL_INJURIES"),
)

vehicle_df = merged_all_df.select(
    col("cv.CRASH_UNIT_ID").alias("CRASH_UNIT_ID_x"),
    col("cv.CRASH_RECORD_ID").alias("CRASH_RECORD_ID"),
    col("cv.CRASH_DATE").alias("CRASH_DATE_x"),
    col("cv.VEHICLE_ID").alias("VEHICLE_ID"),
    col("cv.VEHICLE_MAKE").alias("VEHICLE_MAKE"),
    col("cv.VEHICLE_MODEL").alias("VEHICLE_MODEL"),
    col("cv.VEHICLE_YEAR").alias("VEHICLE_YEAR"),
    col("cv.VEHICLE_TYPE").alias("VEHICLE_TYPE"),
)

person_df = merged_all_df.select(
    col("cv.CRASH_RECORD_ID").alias("CRASH_RECORD_ID"),
    col("cv.CRASH_DATE").alias("CRASH_DATE_x"),
    col("p.PERSON_ID").alias("PERSON_ID"),
    col("p.PERSON_TYPE").alias("PERSON_TYPE"),
    col("cv.VEHICLE_ID").alias("VEHICLE_ID"),
    col("p.PERSON_SEX").alias("PERSON_SEX"),
    col("p.PERSON_AGE").alias("PERSON_AGE"),
)

(
    crash_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", crash_target)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

(
    vehicle_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", vehicle_target)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

(
    person_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", person_target)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

spark.stop()
