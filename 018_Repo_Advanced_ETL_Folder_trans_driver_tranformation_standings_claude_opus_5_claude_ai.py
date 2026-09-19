"""
Business purpose: publish the season standings of every bidder so leaderboard and
agency performance reporting read one ranked row per bidder and year.
Source: warehouse/presentation/auction/lot_sale_results, the Parquet presentation
table of lot sale results.
Target: warehouse/presentation/auction/bidder_standings, a Parquet snapshot
rebuilt on every run.
Output grain: one row per sale_year, bidder_name, bidder_country and agency_name.
Business rules: total_points is the sum of the points scored across the year, wins
counts only the lots taken in first place, and standing_rank orders bidders within
each year by total points and then by wins, sharing a rank on ties.
Generation profile: portable flat batch PySpark aggregation and window ranking.
Runtime arguments: none.
Assumptions: the presentation table is already loaded for the years being ranked,
and the exploratory aggregates, the duplicate window definition and the anti join
of the original were checks that never reached the written output, so they are not
part of this pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, rank, sum as spark_sum, when
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("auction_bidder_standings_presentation").getOrCreate()

# 2. Read only the columns the standings need
lot_sale_results_df = spark.read.parquet("warehouse/presentation/auction/lot_sale_results").select(
    col("sale_year"),
    col("bidder_name"),
    col("bidder_country"),
    col("agency_name"),
    col("score_points"),
    col("final_place"))

# 3. Total the points and count the first places per bidder and year
bidder_totals_df = lot_sale_results_df \
    .groupBy("sale_year", "bidder_name", "bidder_country", "agency_name") \
    .agg(spark_sum("score_points").alias("total_points"),
         count(when(col("final_place") == 1, True)).alias("wins"))

# 4. Rank the bidders within each year
bidder_rank_spec = Window.partitionBy("sale_year").orderBy(desc("total_points"), desc("wins"))

bidder_standings_df = bidder_totals_df \
    .withColumn("standing_rank", rank().over(bidder_rank_spec))

# 5. Load the rebuilt standings into the presentation layer
bidder_standings_df.write \
    .mode("overwrite") \
    .parquet("warehouse/presentation/auction/bidder_standings")

# 6. Release resources
spark.stop()
