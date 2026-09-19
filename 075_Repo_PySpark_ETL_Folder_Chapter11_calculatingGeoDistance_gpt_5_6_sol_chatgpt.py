"""
Business purpose: Enrich NYC ride records with pickup-to-dropoff distance in miles and kilometers, then publish the enriched dataset as compressed CSV.
Source: CSV files under data/landing/geo_distance/uber_data_nyc_2016_06.csv.
Target: Gzip-compressed CSV dataset at data/curated/geo_distance/uber_data_nyc_2016_06_enriched.
Output grain: One output row per input ride record, preserving source columns plus miles and kilometers.
Important rules: Read the source with a header and inferred schema; repartition to four partitions; derive miles from pickup_latitude, pickup_longitude, dropoff_latitude, and dropoff_longitude using the supplied distance SQL expression; derive kilometers from miles using the supplied conversion SQL expression; overwrite the target as gzip-compressed CSV with a header.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (EXTERNALIZED_GEO_DISTANCE_EXPRESSIONS)
Runtime arguments: distance_sql_expression as argv[1]; miles_to_km_sql_expression as argv[2].
Assumptions: The source imports geo.calculateDistance and metricImperial.convert from project modules whose implementations are not included. Their hidden behavior is therefore externalized as required Spark SQL expressions rather than invented. The distance expression may reference pickup_latitude, pickup_longitude, dropoff_latitude, and dropoff_longitude; the conversion expression may reference miles.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("portable_geo_distance_enrichment").getOrCreate()

if len(sys.argv) < 3:
    raise ValueError(
        "Required arguments: distance_sql_expression miles_to_km_sql_expression"
    )

distance_sql_expression = sys.argv[1]
miles_to_km_sql_expression = sys.argv[2]

input_path = "data/landing/geo_distance/uber_data_nyc_2016_06.csv"
output_path = "data/curated/geo_distance/uber_data_nyc_2016_06_enriched"

uber_raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

uber_repartitioned_df = uber_raw_df.repartition(4)

uber_miles_df = uber_repartitioned_df.withColumn(
    "miles",
    expr(distance_sql_expression),
)

uber_kilometers_df = uber_miles_df.withColumn(
    "kilometers",
    expr(miles_to_km_sql_expression),
)

(
    uber_kilometers_df.write
    .mode("overwrite")
    .option("header", True)
    .option("compression", "gzip")
    .csv(output_path)
)

spark.stop()
