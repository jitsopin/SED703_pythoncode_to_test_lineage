"""
Business purpose: Combine green and yellow taxi trip datasets into a common schema, aggregate monthly revenue and trip metrics by pickup zone and service type, and publish the result as Parquet.
Sources: Parquet datasets at data/landing/taxi_revenue/green and data/landing/taxi_revenue/yellow.
Target: Parquet dataset at data/curated/taxi_revenue/monthly_zone_service.
Output grain: One row per pickup zone, revenue month, and service type.
Important rules: Normalize green lpep and yellow tpep pickup/dropoff timestamps to common names; select the same business columns from both services; add service_type; union the datasets; group by PULocationID, month(pickup_datetime), and service_type; calculate the eight monthly revenue sums plus average passenger_count and trip_distance; coalesce to one output partition; overwrite the target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (SQL_AGGREGATION)
Runtime arguments: Optional green input path, yellow input path, and output path.
Assumptions: Source Parquet schemas contain all columns referenced by the original common-column projection and aggregation.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    date_trunc,
    lit,
    sum as spark_sum,
)

spark = SparkSession.builder.appName("portable_taxi_revenue_aggregation").getOrCreate()

green_path = sys.argv[1] if len(sys.argv) > 1 else "data/landing/taxi_revenue/green"
yellow_path = sys.argv[2] if len(sys.argv) > 2 else "data/landing/taxi_revenue/yellow"
output_path = sys.argv[3] if len(sys.argv) > 3 else "data/curated/taxi_revenue/monthly_zone_service"

green_raw_df = spark.read.parquet(green_path)

green_normalized_df = (
    green_raw_df
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
)

yellow_raw_df = spark.read.parquet(yellow_path)

yellow_normalized_df = (
    yellow_raw_df
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
)

common_columns = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "store_and_fwd_flag",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "congestion_surcharge",
]

green_selected_df = green_normalized_df.select(
    *common_columns
).withColumn(
    "service_type",
    lit("green"),
)

yellow_selected_df = yellow_normalized_df.select(
    *common_columns
).withColumn(
    "service_type",
    lit("yellow"),
)

trips_data_df = green_selected_df.unionByName(
    yellow_selected_df
)

revenue_result_df = (
    trips_data_df
    .groupBy(
        col("PULocationID").alias("revenue_zone"),
        date_trunc("month", col("pickup_datetime")).alias("revenue_month"),
        col("service_type"),
    )
    .agg(
        spark_sum("fare_amount").alias("revenue_monthly_fare"),
        spark_sum("extra").alias("revenue_monthly_extra"),
        spark_sum("mta_tax").alias("revenue_monthly_mta_tax"),
        spark_sum("tip_amount").alias("revenue_monthly_tip_amount"),
        spark_sum("tolls_amount").alias("revenue_monthly_tolls_amount"),
        spark_sum("improvement_surcharge").alias("revenue_monthly_improvement_surcharge"),
        spark_sum("total_amount").alias("revenue_monthly_total_amount"),
        spark_sum("congestion_surcharge").alias("revenue_monthly_congestion_surcharge"),
        avg("passenger_count").alias("avg_montly_passenger_count"),
        avg("trip_distance").alias("avg_montly_trip_distance"),
    )
)

single_partition_result_df = revenue_result_df.coalesce(1)

single_partition_result_df.write.mode("overwrite").parquet(output_path)

spark.stop()
