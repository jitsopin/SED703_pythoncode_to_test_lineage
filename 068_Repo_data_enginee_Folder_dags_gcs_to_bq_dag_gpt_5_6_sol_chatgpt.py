"""
Business purpose: Process yellow and green tripdata Parquet files from a portable raw landing area into curated service-specific Parquet datasets partitioned by pickup date.
Sources: Parquet files matching data/landing/tripdata/raw/yellow_tripdata*.parquet and data/landing/tripdata/raw/green_tripdata*.parquet.
Targets: Parquet datasets at data/curated/tripdata/yellow and data/curated/tripdata/green.
Output grain: One row per trip record from each service-specific source.
Important rules: Preserve independent yellow and green branches; treat the original cloud object move as a logical staging step from raw to service-specific paths; use tpep_pickup_datetime for yellow and lpep_pickup_datetime for green; derive pickup_date from the service-specific pickup timestamp; overwrite each curated target partitioned by pickup_date.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (MULTI_BRANCH_PARTITIONED_PARQUET)
Runtime arguments: None.
Assumptions: Airflow scheduling/retry metadata and GCS/BigQuery resource metadata are orchestration details. The external-table plus CREATE OR REPLACE partitioned-table sequence is represented portably as direct Parquet reads followed by overwrite writes partitioned on pickup_date.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("portable_tripdata_partitioned_load").getOrCreate()

yellow_input_path = "data/landing/tripdata/raw/yellow_tripdata*.parquet"
green_input_path = "data/landing/tripdata/raw/green_tripdata*.parquet"
yellow_output_path = "data/curated/tripdata/yellow"
green_output_path = "data/curated/tripdata/green"

yellow_raw_df = spark.read.parquet(yellow_input_path)

yellow_partitioned_df = yellow_raw_df.withColumn(
    "pickup_date",
    to_date(col("tpep_pickup_datetime")),
)

yellow_output_df = yellow_partitioned_df.select(
    *yellow_partitioned_df.columns
)

(
    yellow_output_df.write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .parquet(yellow_output_path)
)

green_raw_df = spark.read.parquet(green_input_path)

green_partitioned_df = green_raw_df.withColumn(
    "pickup_date",
    to_date(col("lpep_pickup_datetime")),
)

green_output_df = green_partitioned_df.select(
    *green_partitioned_df.columns
)

(
    green_output_df.write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .parquet(green_output_path)
)

spark.stop()
