"""
Business purpose: publish the monthly revenue of every departure aerodrome split by
fleet, so charter pricing and fleet planning reporting read one row per aerodrome,
month and fleet.
Sources: data/curated/charter/rotary_flights and
data/curated/charter/fixed_wing_flights, the two curated flight datasets, which
carry the same attributes but name their departure and arrival times after the
fleet.
Target: the table charter_analytics.aerodrome_monthly_revenue, replaced on every
run.
Output grain: one row per departure aerodrome, revenue month and fleet.
Business rules: the two fleets are brought onto one pair of time column names
before they are combined, every row records the fleet it came from, revenue is
totalled per aerodrome and month for each charge component separately, and the
passenger count and the leg distance are averaged over the same grouping.
Generation profile: portable flat batch PySpark union and monthly aggregation.
Runtime arguments: argv[1] and argv[2] are the two flight datasets and argv[3] is
the target table; they fall back to the names above.
Assumptions: both curated datasets are already loaded for the months being
reported; the staging bucket the original configured belongs to its warehouse
connector and has no portable equivalent, so the result is written through the
catalog instead; and the temporary table registration and the union call of the
original are both deprecated, so the aggregation is expressed through the
DataFrame API.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, date_trunc, lit, sum as spark_sum

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("charter_aerodrome_monthly_revenue").getOrCreate()
rotary_input = sys.argv[1] if len(sys.argv) > 1 else "data/curated/charter/rotary_flights"
fixed_wing_input = sys.argv[2] if len(sys.argv) > 2 else "data/curated/charter/fixed_wing_flights"
revenue_table = sys.argv[3] if len(sys.argv) > 3 else "charter_analytics.aerodrome_monthly_revenue"

# 2. Read the rotary fleet
rotary_flight_df = spark.read.parquet(rotary_input).select(
    col("operator_id"),
    col("rotary_departure_time"),
    col("rotary_arrival_time"),
    col("offline_filed_flag"),
    col("tariff_code_id"),
    col("departure_aerodrome_id"),
    col("arrival_aerodrome_id"),
    col("passenger_count"),
    col("leg_distance_nm"),
    col("charter_fee"),
    col("extra_charge"),
    col("airport_tax"),
    col("crew_gratuity"),
    col("handling_fee"),
    col("infrastructure_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("slot_surcharge"))

# 3. Put the rotary times under the shared names
rotary_renamed_df = rotary_flight_df \
    .withColumnRenamed("rotary_departure_time", "departure_timestamp") \
    .withColumnRenamed("rotary_arrival_time", "arrival_timestamp")

# 4. Keep the shared columns and tag the fleet
rotary_selected_df = rotary_renamed_df.select(
    col("operator_id"),
    col("departure_timestamp"),
    col("arrival_timestamp"),
    col("offline_filed_flag"),
    col("tariff_code_id"),
    col("departure_aerodrome_id"),
    col("arrival_aerodrome_id"),
    col("passenger_count"),
    col("leg_distance_nm"),
    col("charter_fee"),
    col("extra_charge"),
    col("airport_tax"),
    col("crew_gratuity"),
    col("handling_fee"),
    col("infrastructure_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("slot_surcharge")) \
    .withColumn("service_type", lit("rotary"))

# 5. Read the fixed wing fleet
fixed_wing_flight_df = spark.read.parquet(fixed_wing_input).select(
    col("operator_id"),
    col("fixed_wing_departure_time"),
    col("fixed_wing_arrival_time"),
    col("offline_filed_flag"),
    col("tariff_code_id"),
    col("departure_aerodrome_id"),
    col("arrival_aerodrome_id"),
    col("passenger_count"),
    col("leg_distance_nm"),
    col("charter_fee"),
    col("extra_charge"),
    col("airport_tax"),
    col("crew_gratuity"),
    col("handling_fee"),
    col("infrastructure_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("slot_surcharge"))

# 6. Put the fixed wing times under the shared names
fixed_wing_renamed_df = fixed_wing_flight_df \
    .withColumnRenamed("fixed_wing_departure_time", "departure_timestamp") \
    .withColumnRenamed("fixed_wing_arrival_time", "arrival_timestamp")

# 7. Keep the shared columns and tag the fleet
fixed_wing_selected_df = fixed_wing_renamed_df.select(
    col("operator_id"),
    col("departure_timestamp"),
    col("arrival_timestamp"),
    col("offline_filed_flag"),
    col("tariff_code_id"),
    col("departure_aerodrome_id"),
    col("arrival_aerodrome_id"),
    col("passenger_count"),
    col("leg_distance_nm"),
    col("charter_fee"),
    col("extra_charge"),
    col("airport_tax"),
    col("crew_gratuity"),
    col("handling_fee"),
    col("infrastructure_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("slot_surcharge")) \
    .withColumn("service_type", lit("fixed_wing"))

# 8. Bring both fleets together
flight_df = rotary_selected_df.unionByName(fixed_wing_selected_df)

# 9. Place every flight in the month it departed
flight_monthly_df = flight_df \
    .withColumn("revenue_month", date_trunc("month", col("departure_timestamp")))

# 10. Total the revenue per aerodrome, month and fleet
aerodrome_monthly_revenue_df = flight_monthly_df \
    .groupBy("departure_aerodrome_id", "revenue_month", "service_type") \
    .agg(spark_sum("charter_fee").alias("revenue_monthly_charter_fee"),
         spark_sum("extra_charge").alias("revenue_monthly_extra"),
         spark_sum("airport_tax").alias("revenue_monthly_airport_tax"),
         spark_sum("crew_gratuity").alias("revenue_monthly_crew_gratuity"),
         spark_sum("handling_fee").alias("revenue_monthly_handling_fee"),
         spark_sum("infrastructure_surcharge").alias("revenue_monthly_infrastructure_surcharge"),
         spark_sum("total_amount").alias("revenue_monthly_total"),
         spark_sum("slot_surcharge").alias("revenue_monthly_slot_surcharge"),
         avg("passenger_count").alias("avg_monthly_passenger_count"),
         avg("leg_distance_nm").alias("avg_monthly_leg_distance"))

# 11. Replace the published revenue table
aerodrome_monthly_revenue_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable(revenue_table)

# 12. Release resources
spark.stop()
