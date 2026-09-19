"""Business purpose: Consolidate green and yellow taxi trips to compute monthly revenue metrics for BigQuery.
Source 1: gs://dtc-data-lake-bucketname/landing/taxis/green_trips (Parquet)
Source 2: gs://dtc-data-lake-bucketname/landing/taxis/yellow_trips (Parquet)
Target: BigQuery - taxi_dataset.monthly_revenue_report
Output grain: One record per pickup location, month, and service type.
Business rules: Unify green and yellow taxi datasets, standardize datetime columns, label service types, compute monthly financial metrics, and load into a BigQuery data warehouse.
Generation profile: Portable PySpark script replacing argparse dependencies and deprecated temporary table registration while retaining BigQuery sink integration.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialize SparkSession and configure BigQuery temporary GCS bucket
spark = SparkSession.builder \
    .appName("TaxiMonthlyRevenueBigQueryETL") \
    .config("temporaryGcsBucket", "dataproc-temp-europe-west6-828225226997-fckhkym8") \
    .getOrCreate()

# 2. Extract and project Green Taxi Data
df_green = spark.read.parquet("gs://dtc-data-lake-bucketname/landing/taxis/green_trips") \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

# 3. Extract and project Yellow Taxi Data
df_yellow = spark.read.parquet("gs://dtc-data-lake-bucketname/landing/taxis/yellow_trips") \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# 4. Define common schema boundaries and tag service types
common_columns = [
    "VendorID", "pickup_datetime", "dropoff_datetime", "store_and_fwd_flag",
    "RatecodeID", "PULocationID", "DOLocationID", "passenger_count",
    "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "payment_type",
    "congestion_surcharge"
]

df_green_sel = df_green.select(common_columns).withColumn("service_type", F.lit("green"))
df_yellow_sel = df_yellow.select(common_columns).withColumn("service_type", F.lit("yellow"))

# 5. Union datasets and register temporary view
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.createOrReplaceTempView("trips_data")

# 6. Aggregate monthly revenue and operational metrics
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# 7. Load aggregated report to Google BigQuery
df_result.write.format("bigquery") \
    .mode("overwrite") \
    .option("table", "taxi_dataset.monthly_revenue_report") \
    .save()

# 8. Release resources
spark.stop()