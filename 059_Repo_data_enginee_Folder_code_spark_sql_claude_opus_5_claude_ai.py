"""
Business purpose: publish the monthly revenue of every departure station split by
service, so pricing and timetable reporting read one row per station, month and
service.
Sources: data/curated/rail/regional_journeys and data/curated/rail/intercity_
journeys, the two curated journey datasets, which carry the same attributes but
name their boarding and alighting times after the service.
Target: warehouse/presentation/rail/station_monthly_revenue, a Parquet dataset
partitioned by revenue_month and replaced on every run.
Output grain: one row per revenue_station, revenue_month and service_type.
Business rules: both services are brought onto one set of column names and
combined in the same statement that totals them, every row records the service it
came from, revenue is totalled per station and month for each charge component
separately, and the passenger count and the journey distance are averaged over the
same grouping.
Generation profile: portable flat batch PySpark SQL union and monthly aggregation.
Runtime arguments: argv[1] and argv[2] are the two journey datasets and argv[3] is
the target; they fall back to the paths named above.
Assumptions: both curated datasets are already loaded for the months being
reported; the temporary table registration and the union call of the original are
both deprecated, so each service is exposed as a view and the two are combined
inside the statement that aggregates them.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("rail_station_monthly_revenue").getOrCreate()
regional_input = sys.argv[1] if len(sys.argv) > 1 else "data/curated/rail/regional_journeys"
intercity_input = sys.argv[2] if len(sys.argv) > 2 else "data/curated/rail/intercity_journeys"
revenue_output = sys.argv[3] if len(sys.argv) > 3 else \
    "warehouse/presentation/rail/station_monthly_revenue"

# 2. Read the regional journeys
regional_journey_df = spark.read.parquet(regional_input).select(
    col("operator_id"),
    col("regional_boarding_time"),
    col("regional_alighting_time"),
    col("offline_flag"),
    col("fare_class_id"),
    col("departure_station_id"),
    col("arrival_station_id"),
    col("passenger_count"),
    col("journey_distance_km"),
    col("fare_amount"),
    col("extra_charge"),
    col("network_levy"),
    col("tip_amount"),
    col("access_fee"),
    col("station_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("peak_surcharge"))

# 3. Expose the regional journeys to SQL
regional_journey_df.createOrReplaceTempView("vw_regional_journey")

# 4. Read the intercity journeys
intercity_journey_df = spark.read.parquet(intercity_input).select(
    col("operator_id"),
    col("intercity_boarding_time"),
    col("intercity_alighting_time"),
    col("offline_flag"),
    col("fare_class_id"),
    col("departure_station_id"),
    col("arrival_station_id"),
    col("passenger_count"),
    col("journey_distance_km"),
    col("fare_amount"),
    col("extra_charge"),
    col("network_levy"),
    col("tip_amount"),
    col("access_fee"),
    col("station_surcharge"),
    col("total_amount"),
    col("payment_type_id"),
    col("peak_surcharge"))

# 5. Expose the intercity journeys to SQL
intercity_journey_df.createOrReplaceTempView("vw_intercity_journey")

# 6. Combine both services and total the revenue in one statement
station_monthly_revenue_df = spark.sql("""
    SELECT
        journey.departure_station_id AS revenue_station,
        date_trunc('month', journey.boarding_time) AS revenue_month,
        journey.service_type,
        SUM(journey.fare_amount) AS revenue_monthly_fare,
        SUM(journey.extra_charge) AS revenue_monthly_extra,
        SUM(journey.network_levy) AS revenue_monthly_network_levy,
        SUM(journey.tip_amount) AS revenue_monthly_tip,
        SUM(journey.access_fee) AS revenue_monthly_access_fee,
        SUM(journey.station_surcharge) AS revenue_monthly_station_surcharge,
        SUM(journey.total_amount) AS revenue_monthly_total,
        SUM(journey.peak_surcharge) AS revenue_monthly_peak_surcharge,
        AVG(journey.passenger_count) AS avg_monthly_passenger_count,
        AVG(journey.journey_distance_km) AS avg_monthly_journey_distance
    FROM (
        SELECT regional_boarding_time AS boarding_time,
               departure_station_id,
               passenger_count,
               journey_distance_km,
               fare_amount,
               extra_charge,
               network_levy,
               tip_amount,
               access_fee,
               station_surcharge,
               total_amount,
               peak_surcharge,
               'regional' AS service_type
          FROM vw_regional_journey
        UNION ALL
        SELECT intercity_boarding_time AS boarding_time,
               departure_station_id,
               passenger_count,
               journey_distance_km,
               fare_amount,
               extra_charge,
               network_levy,
               tip_amount,
               access_fee,
               station_surcharge,
               total_amount,
               peak_surcharge,
               'intercity' AS service_type
          FROM vw_intercity_journey
    ) journey
    GROUP BY 1, 2, 3
""")

# 7. Replace the published revenue table, split by month
station_monthly_revenue_df.write \
    .mode("overwrite") \
    .partitionBy("revenue_month") \
    .parquet(revenue_output)

# 8. Release resources
spark.stop()
