"""
Business purpose: Ingest monthly NYC taxi trip CSV-gzip files, apply stable taxi data types and service-aware timestamps, and publish a partitioned Parquet dataset while avoiding duplicate reprocessing of already materialized month partitions.
Sources: Monthly green-taxi CSV-gzip files under data/landing/nyc_taxi/green/<year>/ named green_tripdata_<year>-<month>.csv.gz for 2019, 2020, and 2021.
Target: Parquet dataset at data/curated/nyc_taxi/trips partitioned by service, year, and month.
Output grain: One row per taxi trip record.
Important rules: Preserve the visible green-taxi runs for 2019-2021; derive service/year/month from each source path; cast the configured identifier/count and monetary/distance columns; use lpep pickup/dropoff timestamps for green service; ignore already-produced month partitions by writing only source months whose target partition is absent; publish Parquet partitioned by service/year/month.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (MONTHLY_TAXI_INCREMENTAL_INGEST)
Runtime arguments: None.
Assumptions: HTTP download, progress bars, local file cache checks, PyArrow chunk writers, cloud credentials, and object-store existence checks are environment orchestration. This portable rewrite models the observable skip-existing behavior against the local curated target and preserves the data transformation semantics in Spark.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, regexp_extract, to_timestamp

spark = SparkSession.builder.appName("portable_monthly_taxi_incremental_ingest").getOrCreate()

input_glob = "data/landing/nyc_taxi/green/{2019,2020,2021}/green_tripdata_*.csv.gz"
output_path = "data/curated/nyc_taxi/trips"

taxi_raw_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .csv(input_glob)
    .withColumn("_source_file", input_file_name())
)

taxi_partitioned_df = (
    taxi_raw_df
    .withColumn("service", regexp_extract(col("_source_file"), r"/([^/]+)/\d{4}/[^/]+$", 1))
    .withColumn("year", regexp_extract(col("_source_file"), r"/(\d{4})/[^/]+$", 1).cast("int"))
    .withColumn("month", regexp_extract(col("_source_file"), r"_(\d{4})-(\d{2})\.csv\.gz$", 2).cast("int"))
)

taxi_typed_df = (
    taxi_partitioned_df
    .withColumn("VendorID", col("VendorID").cast("int"))
    .withColumn("RatecodeID", col("RatecodeID").cast("int"))
    .withColumn("PULocationID", col("PULocationID").cast("int"))
    .withColumn("DOLocationID", col("DOLocationID").cast("int"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("payment_type", col("payment_type").cast("int"))
    .withColumn("trip_type", col("trip_type").cast("int"))
    .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast("string"))
    .withColumn("trip_distance", col("trip_distance").cast("double"))
    .withColumn("fare_amount", col("fare_amount").cast("double"))
    .withColumn("extra", col("extra").cast("double"))
    .withColumn("mta_tax", col("mta_tax").cast("double"))
    .withColumn("tip_amount", col("tip_amount").cast("double"))
    .withColumn("tolls_amount", col("tolls_amount").cast("double"))
    .withColumn("ehailfee", col("ehailfee").cast("double"))
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast("double"))
    .withColumn("total_amount", col("total_amount").cast("double"))
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double"))
)

taxi_dates_df = (
    taxi_typed_df
    .withColumn("pickup_datetime", to_timestamp(col("lpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("lpep_dropoff_datetime")))
)

existing_partitions = {
    (int(year_dir.name.split("=")[1]), int(month_dir.name.split("=")[1]))
    for year_dir in Path(output_path, "service=green").glob("year=*")
    for month_dir in year_dir.glob("month=*")
    if year_dir.name.split("=")[1].isdigit() and month_dir.name.split("=")[1].isdigit()
}

taxi_incremental_df = taxi_dates_df.filter(
    ~col("year").isin([year for year, _ in existing_partitions])
    | ~col("month").isin([month for _, month in existing_partitions])
)

taxi_output_df = taxi_incremental_df.drop("_source_file")

(
    taxi_output_df.write
    .mode("append")
    .partitionBy("service", "year", "month")
    .parquet(output_path)
)

spark.stop()
