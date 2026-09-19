"""
Business purpose: publish the monthly revenue of every start zone split by vehicle
type, so pricing and fleet placement reporting read one row per zone, month and
vehicle type.
Sources: data/curated/micromobility/scooter_trips and
data/curated/micromobility/bike_trips, the two curated trip datasets, which carry
the same attributes under vehicle specific names for the start and end times.
Target: warehouse/presentation/micromobility/zone_monthly_revenue, a Parquet
dataset written as one file and replaced on every run.
Output grain: one row per revenue_zone, revenue_month and service_type.
Business rules: both fleets are brought onto one set of column names before they
are combined, every row records the vehicle type it came from, revenue is totalled
per zone and month for each charge component separately, and the rider count and
the trip distance are averaged over the same grouping.
Generation profile: portable flat batch PySpark union and monthly aggregation.
Runtime arguments: argv[1] and argv[2] are the two trip datasets and argv[3] is the
target; they fall back to the paths named above.
Assumptions: both curated datasets are already loaded for the months being
reported; the temporary table registration of the original uses the current view
API, and the union uses the by name form, because both of the calls it used are
deprecated.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("micromobility_zone_monthly_revenue").getOrCreate()
scooter_input = sys.argv[1] if len(sys.argv) > 1 else "data/curated/micromobility/scooter_trips"
bike_input = sys.argv[2] if len(sys.argv) > 2 else "data/curated/micromobility/bike_trips"
revenue_output = sys.argv[3] if len(sys.argv) > 3 else \
    "warehouse/presentation/micromobility/zone_monthly_revenue"

# 2. Read the scooter trips
scooter_trip_df = spark.read.parquet(scooter_input).select(
    col("operatorId"),
    col("scooterStartTime"),
    col("scooterEndTime"),
    col("offlineFlag"),
    col("ratePlanId"),
    col("startZoneId"),
    col("endZoneId"),
    col("riderCount"),
    col("tripDistanceKm"),
    col("fareAmount"),
    col("extraCharge"),
    col("cityTax"),
    col("tipAmount"),
    col("tollAmount"),
    col("serviceSurcharge"),
    col("totalAmount"),
    col("paymentTypeId"),
    col("congestionSurcharge"))

# 3. Bring the scooter trips onto the common names and tag the vehicle type
scooter_selected_df = scooter_trip_df.select(
    col("operatorId").alias("operator_id"),
    col("scooterStartTime").alias("start_timestamp"),
    col("scooterEndTime").alias("end_timestamp"),
    col("offlineFlag").alias("offline_flag"),
    col("ratePlanId").alias("rate_plan_id"),
    col("startZoneId").alias("start_zone_id"),
    col("endZoneId").alias("end_zone_id"),
    col("riderCount").alias("rider_count"),
    col("tripDistanceKm").alias("trip_distance_km"),
    col("fareAmount").alias("fare_amount"),
    col("extraCharge").alias("extra_charge"),
    col("cityTax").alias("city_tax"),
    col("tipAmount").alias("tip_amount"),
    col("tollAmount").alias("toll_amount"),
    col("serviceSurcharge").alias("service_surcharge"),
    col("totalAmount").alias("total_amount"),
    col("paymentTypeId").alias("payment_type_id"),
    col("congestionSurcharge").alias("congestion_surcharge"),
    lit("scooter").alias("service_type"))

# 4. Read the bike trips
bike_trip_df = spark.read.parquet(bike_input).select(
    col("operatorId"),
    col("bikeStartTime"),
    col("bikeEndTime"),
    col("offlineFlag"),
    col("ratePlanId"),
    col("startZoneId"),
    col("endZoneId"),
    col("riderCount"),
    col("tripDistanceKm"),
    col("fareAmount"),
    col("extraCharge"),
    col("cityTax"),
    col("tipAmount"),
    col("tollAmount"),
    col("serviceSurcharge"),
    col("totalAmount"),
    col("paymentTypeId"),
    col("congestionSurcharge"))

# 5. Bring the bike trips onto the common names and tag the vehicle type
bike_selected_df = bike_trip_df.select(
    col("operatorId").alias("operator_id"),
    col("bikeStartTime").alias("start_timestamp"),
    col("bikeEndTime").alias("end_timestamp"),
    col("offlineFlag").alias("offline_flag"),
    col("ratePlanId").alias("rate_plan_id"),
    col("startZoneId").alias("start_zone_id"),
    col("endZoneId").alias("end_zone_id"),
    col("riderCount").alias("rider_count"),
    col("tripDistanceKm").alias("trip_distance_km"),
    col("fareAmount").alias("fare_amount"),
    col("extraCharge").alias("extra_charge"),
    col("cityTax").alias("city_tax"),
    col("tipAmount").alias("tip_amount"),
    col("tollAmount").alias("toll_amount"),
    col("serviceSurcharge").alias("service_surcharge"),
    col("totalAmount").alias("total_amount"),
    col("paymentTypeId").alias("payment_type_id"),
    col("congestionSurcharge").alias("congestion_surcharge"),
    lit("bike").alias("service_type"))

# 6. Bring both fleets together
trip_df = scooter_selected_df.unionByName(bike_selected_df)

# 7. Expose the combined trips to SQL
trip_df.createOrReplaceTempView("vw_micromobility_trip")

# 8. Total the revenue per zone, month and vehicle type
zone_monthly_revenue_df = spark.sql("""
    SELECT
        start_zone_id AS revenue_zone,
        date_trunc('month', start_timestamp) AS revenue_month,
        service_type,
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra_charge) AS revenue_monthly_extra,
        SUM(city_tax) AS revenue_monthly_city_tax,
        SUM(tip_amount) AS revenue_monthly_tip,
        SUM(toll_amount) AS revenue_monthly_toll,
        SUM(service_surcharge) AS revenue_monthly_service_surcharge,
        SUM(total_amount) AS revenue_monthly_total,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
        AVG(rider_count) AS avg_monthly_rider_count,
        AVG(trip_distance_km) AS avg_monthly_trip_distance
    FROM vw_micromobility_trip
    GROUP BY 1, 2, 3
""")

# 9. Gather the result into a single file
zone_monthly_revenue_single_df = zone_monthly_revenue_df.coalesce(1)

# 10. Replace the published revenue table
zone_monthly_revenue_single_df.write \
    .mode("overwrite") \
    .parquet(revenue_output)

# 11. Release resources
spark.stop()
