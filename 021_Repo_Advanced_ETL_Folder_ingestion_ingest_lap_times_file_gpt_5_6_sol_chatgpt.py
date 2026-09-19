"""
Business purpose: Build a portable curated lap-performance table from a folder of CSV files.
Source: CSV lap observations under data/landing/lap_performance/lap_observations/.
Target: Managed Parquet table analytics_curated.lap_performance.
Output grain: One row per event_id, participant_id, and lap_number from the source folder.
Important rules: Read all CSV files in the landing folder, standardize identifiers, attach runtime data_source, add ingestion timestamp, and overwrite the curated table.
Generation profile: DATAFRAME_CHAIN + TABLE_LAP_FLOW
Runtime arguments: Optional first command-line argument supplies data_source; defaults to "unspecified".
Assumptions: Every CSV file in the landing folder conforms to the declared schema and a Spark catalog supporting saveAsTable is available.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_lap_performance_table_pipeline").getOrCreate()

data_source = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
source_path = "data/landing/lap_performance/lap_observations"
target_table = "analytics_curated.lap_performance"

lap_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("participant_id", IntegerType(), True),
    StructField("lap_number", IntegerType(), True),
    StructField("running_position", IntegerType(), True),
    StructField("lap_time_text", StringType(), True),
    StructField("lap_milliseconds", IntegerType(), True),
])

# 2. Extract
lap_raw_df = (
    spark.read
    .schema(lap_schema)
    .csv(source_path)
)

# 3. Add ingestion metadata
lap_ingested_df = lap_raw_df.select(
    col("event_id"),
    col("participant_id"),
    col("lap_number"),
    col("running_position"),
    col("lap_time_text"),
    col("lap_milliseconds"),
    current_timestamp().alias("ingested_at_utc"),
)

# 4. Add runtime source metadata
published_lap_df = lap_ingested_df.select(
    col("event_id"),
    col("participant_id"),
    col("lap_number"),
    col("running_position"),
    col("lap_time_text"),
    col("lap_milliseconds"),
    col("ingested_at_utc"),
    lit(data_source).alias("data_source"),
)

# 5. Load
published_lap_df.write.mode("overwrite").format("parquet").saveAsTable(target_table)

# 6. Release resources
spark.stop()
