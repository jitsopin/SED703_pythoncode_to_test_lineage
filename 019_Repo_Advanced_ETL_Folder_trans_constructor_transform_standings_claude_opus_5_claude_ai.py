"""
Business purpose: publish the season standings of every selling agency so
leaderboard and commission reporting read one ranked row per agency and year.
Source: warehouse/presentation/auction/lot_sale_results, the Parquet presentation
table of lot sale results.
Target: warehouse/presentation/auction/agency_standings, a Parquet snapshot
rebuilt on every run.
Output grain: one row per sale_year and agency_name.
Business rules: total_points is the sum of the points scored across the year, wins
counts only the lots taken in first place, and standing_rank orders agencies
within each year by total points alone, sharing a rank on ties.
Generation profile: portable flat batch PySpark aggregation and window ranking.
Runtime arguments: none.
Assumptions: the presentation table is already loaded for the years being ranked,
and the temporary views, the ad hoc SQL selects and the extra Delta write of the
original were exploration around the published output, so they are not part of
this pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, rank, sum as spark_sum, when
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("auction_agency_standings_presentation").getOrCreate()

# 2. Read only the columns the standings need
lot_sale_results_df = spark.read.parquet("warehouse/presentation/auction/lot_sale_results").select(
    col("sale_year"),
    col("agency_name"),
    col("score_points"),
    col("final_place"))

# 3. Total the points and count the first places per agency and year
agency_totals_df = lot_sale_results_df \
    .groupBy("sale_year", "agency_name") \
    .agg(spark_sum("score_points").alias("total_points"),
         count(when(col("final_place") == 1, True)).alias("wins"))

# 4. Rank the agencies within each year
agency_rank_spec = Window.partitionBy("sale_year").orderBy(desc("total_points"))

agency_standings_df = agency_totals_df \
    .withColumn("standing_rank", rank().over(agency_rank_spec))

# 5. Load the rebuilt standings into the presentation layer
agency_standings_df.write \
    .mode("overwrite") \
    .parquet("warehouse/presentation/auction/agency_standings")

# 6. Release resources
spark.stop()
