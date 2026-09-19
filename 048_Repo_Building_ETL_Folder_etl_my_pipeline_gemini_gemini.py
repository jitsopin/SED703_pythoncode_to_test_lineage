"""Business purpose: Consolidate and normalize traffic crash reports, vehicles, and involved persons.
Source: data/landing/traffic/traffic_crashes.csv, traffic_vehicles.csv, traffic_people.csv
Target: Operational PostgreSQL Database (JDBC) - crash, vehicle, and person tables
Output grain: Normalized relational records grouped by crash incidents, vehicles, and individuals.
Business rules: Left join crashes to vehicles and people on CRASH_RECORD_ID, project required columns, and append to relational tables.
Generation profile: Portable PySpark script replacing Bonobo ETL, Pandas, and Psycopg2 dependencies.
"""

from pyspark.sql import SparkSession

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("TrafficCrashRelationalETL").getOrCreate()

# 2. Extract raw traffic data
crashes_df = spark.read.option("header", "true").csv("data/landing/traffic/traffic_crashes.csv")
vehicles_df = spark.read.option("header", "true").csv("data/landing/traffic/traffic_vehicles.csv")
people_df = spark.read.option("header", "true").csv("data/landing/traffic/traffic_people.csv")

# 3. Transform: Merge datasets via left joins
crash_vehicles_df = crashes_df.join(vehicles_df, on="CRASH_RECORD_ID", how="left")
merged_all_df = crash_vehicles_df.join(people_df, on="CRASH_RECORD_ID", how="left")

# 4. Transform: Project normalized tables
crash_final_df = merged_all_df.select(
    "CRASH_UNIT_ID_x", "CRASH_RECORD_ID", "CRASH_DATE_x",
    "PERSON_ID", "VEHICLE_ID", "NUM_UNITS", "TOTAL_INJURIES"
)

vehicle_final_df = merged_all_df.select(
    "CRASH_UNIT_ID_x", "CRASH_RECORD_ID", "CRASH_DATE_x",
    "VEHICLE_ID", "VEHICLE_MAKE", "VEHICLE_MODEL", "VEHICLE_YEAR", "VEHICLE_TYPE"
)

person_final_df = merged_all_df.select(
    "CRASH_RECORD_ID", "CRASH_DATE_x", "PERSON_ID", "PERSON_TYPE",
    "VEHICLE_ID", "PERSON_SEX", "PERSON_AGE"
)

# 5. Load: Configure JDBC properties
jdbc_url = "jdbc:postgresql://mydbinstance.cmyxyz123.us-east-1.rds.amazonaws.com:5432/traffic_db"
jdbc_properties = {
    "user": "db_user",
    "password": "db_password",
    "driver": "org.postgresql.Driver"
}

# 6. Load: Append projected tables to PostgreSQL
crash_final_df.write.mode("append").jdbc(url=jdbc_url, table="public.crash", properties=jdbc_properties)
vehicle_final_df.write.mode("append").jdbc(url=jdbc_url, table="public.vehicle", properties=jdbc_properties)
person_final_df.write.mode("append").jdbc(url=jdbc_url, table="public.person", properties=jdbc_properties)

# 7. Release resources
spark.stop()