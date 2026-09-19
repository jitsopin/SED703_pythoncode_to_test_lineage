"""Business purpose: Ingest raw parquet taxi trips from GCS into partitioned BigQuery tables.
Source: gs://dtc-data-lake-bucketname/raw/ (Parquet format)
Target: BigQuery - trips_data_all.green_tripdata and trips_data_all.yellow_tripdata
Output grain: One record per taxi trip.
Business rules: Read raw files directly from the data lake and write to native BigQuery tables partitioned by the respective pickup datetime columns.
Generation profile: Portable PySpark script replacing Airflow DAGs, GCS move operators, and BQ external table CTAS queries.
"""

from pyspark.sql import SparkSession

# 1. Initialize SparkSession and configure BigQuery temporary GCS bucket
spark = SparkSession.builder \
    .appName("GCSToBigQueryTaxiETL") \
    .config("temporaryGcsBucket", "dtc-data-lake-bucketname-temp") \
    .getOrCreate()

# 2. Extract and Load Green Taxi Data
green_raw_df = spark.read.parquet("gs://dtc-data-lake-bucketname/raw/green_tripdata*.parquet")

green_raw_df.write.format("bigquery") \
    .mode("overwrite") \
    .option("table", "trips_data_all.green_tripdata") \
    .option("partitionField", "lpep_pickup_datetime") \
    .option("partitionType", "DAY") \
    .save()

# 3. Extract and Load Yellow Taxi Data
yellow_raw_df = spark.read.parquet("gs://dtc-data-lake-bucketname/raw/yellow_tripdata*.parquet")

yellow_raw_df.write.format("bigquery") \
    .mode("overwrite") \
    .option("table", "trips_data_all.yellow_tripdata") \
    .option("partitionField", "tpep_pickup_datetime") \
    .option("partitionType", "DAY") \
    .save()

# 4. Release resources
spark.stop()