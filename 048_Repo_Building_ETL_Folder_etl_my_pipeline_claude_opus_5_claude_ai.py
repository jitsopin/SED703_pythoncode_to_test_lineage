"""
Business purpose: split the safety incident feed into the three tables the
reporting database holds, so each of them carries the incident context already
resolved.
Sources: data/landing/safety/incident_reports.csv,
data/landing/safety/involved_equipment.csv and
data/landing/safety/involved_persons.csv, all with a header row and all keyed by
the incident record.
Targets: the tables safety_reporting.incident, safety_reporting.equipment and
safety_reporting.person in the reporting database, appended over JDBC.
Output grain: one row per incident record joined with the equipment and the person
involved, published three times under three column sets.
Business rules: an incident is kept even when no equipment or no person is
recorded against it, so both joins are left joins; the unit and the date published
in all three tables are the ones the incident report carries, not the ones the
equipment or person rows carry; and each table keeps only the columns its
consumers read.
Generation profile: portable flat batch PySpark join and three way split.
Runtime arguments: argv[1] is the JDBC url of the reporting database, argv[2] the
user and argv[3] the password; they fall back to a local database, "postgres" and
an empty password.
Assumptions: the original is an outline in which two of the three reads and two of
the three loads are left as a note to repeat the first, so all three are written
out here; the columns it took with an _x suffix are the ones the merge kept from
the incident side, so the colliding columns of the other two feeds are named apart
when they are read rather than relying on suffixes; the equipment reference in the
person table is the one the person feed carries, which the original could not have
selected unqualified after its second merge; and credentials are taken from the
arguments instead of being written into the script.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("safety_incident_reporting_load").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/reporting"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "postgres"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the incident reports
incident_report_schema = StructType(fields=[
    StructField("incident_record_id", StringType(), True),
    StructField("report_unit_id", IntegerType(), True),
    StructField("incident_date", DateType(), True),
    StructField("unit_count", IntegerType(), True),
    StructField("total_injuries", IntegerType(), True),
])

incident_report_df = spark.read \
    .option("header", True) \
    .schema(incident_report_schema) \
    .csv("data/landing/safety/incident_reports.csv")

# 3. Read the equipment involved, keeping its own unit and date apart
involved_equipment_schema = StructType(fields=[
    StructField("incident_record_id", StringType(), True),
    StructField("equipment_unit_id", IntegerType(), True),
    StructField("equipment_recorded_date", DateType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("equipment_make", StringType(), True),
    StructField("equipment_model", StringType(), True),
    StructField("equipment_year", IntegerType(), True),
    StructField("equipment_type", StringType(), True),
])

involved_equipment_df = spark.read \
    .option("header", True) \
    .schema(involved_equipment_schema) \
    .csv("data/landing/safety/involved_equipment.csv")

# 4. Read the people involved, keeping their own date and equipment apart
involved_person_schema = StructType(fields=[
    StructField("incident_record_id", StringType(), True),
    StructField("person_recorded_date", DateType(), True),
    StructField("person_id", StringType(), True),
    StructField("person_role", StringType(), True),
    StructField("person_equipment_id", StringType(), True),
    StructField("person_gender", StringType(), True),
    StructField("person_age", IntegerType(), True),
])

involved_person_df = spark.read \
    .option("header", True) \
    .schema(involved_person_schema) \
    .csv("data/landing/safety/involved_persons.csv")

# 5. Attach the equipment to its incident
incident_equipment_df = incident_report_df.join(
    involved_equipment_df, ["incident_record_id"], "left_outer")

# 6. Attach the people to the same incident
incident_full_df = incident_equipment_df.join(
    involved_person_df, ["incident_record_id"], "left_outer")

# 7. Build the incident table
incident_df = incident_full_df.select(
    col("report_unit_id"),
    col("incident_record_id"),
    col("incident_date"),
    col("person_id"),
    col("equipment_id"),
    col("unit_count"),
    col("total_injuries"))

# 8. Append the incident table
incident_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "safety_reporting.incident") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("append") \
    .save()

# 9. Build the equipment table
equipment_df = incident_full_df.select(
    col("report_unit_id"),
    col("incident_record_id"),
    col("incident_date"),
    col("equipment_id"),
    col("equipment_make"),
    col("equipment_model"),
    col("equipment_year"),
    col("equipment_type"))

# 10. Append the equipment table
equipment_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "safety_reporting.equipment") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("append") \
    .save()

# 11. Build the person table
person_df = incident_full_df.select(
    col("incident_record_id"),
    col("incident_date"),
    col("person_id"),
    col("person_role"),
    col("person_equipment_id"),
    col("person_gender"),
    col("person_age"))

# 12. Append the person table
person_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "safety_reporting.person") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("append") \
    .save()

# 13. Release resources
spark.stop()
