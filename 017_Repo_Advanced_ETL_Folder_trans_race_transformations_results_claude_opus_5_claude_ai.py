"""
Business purpose: publish one presentation table of lot sale results so auction
performance reporting reads bidder, agency, saleroom and event context from a
single place.
Sources: warehouse/curated/auction/lot_outcomes, sale_event_schedule,
saleroom_directory, bidder_directory and agency_master, all Parquet datasets of
the curated layer.
Target: warehouse/presentation/auction/lot_sale_results, a Parquet snapshot
rebuilt on every run.
Output grain: one row per lot outcome, since every join reduces a dimension to at
most one matching row.
Business rules: a sale event must resolve to a saleroom, and a lot outcome must
resolve to its event, bidder and agency, so every join is an inner join; each load
carries its own creation timestamp.
Generation profile: portable flat batch PySpark star-join presentation build.
Runtime arguments: none.
Assumptions: the curated datasets are already loaded for the period being
published, their key columns are unique, and the joins are inner because that is
what the original ran despite its comment describing a left join.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 1. Initialize Spark
spark = SparkSession.builder.appName("auction_lot_sale_results_presentation").getOrCreate()

# 2. Read the bidder dimension with business-qualified names
bidder_df = spark.read.parquet("warehouse/curated/auction/bidder_directory").select(
    col("bidder_id").alias("bidder_key"),
    col("number").alias("bidder_number"),
    col("name").alias("bidder_name"),
    col("country").alias("bidder_country"))

# 3. Read the agency dimension with business-qualified names
agency_df = spark.read.parquet("warehouse/curated/auction/agency_master").select(
    col("agency_id").alias("agency_key"),
    col("name").alias("agency_name"))

# 4. Read the saleroom dimension with business-qualified names
saleroom_df = spark.read.parquet("warehouse/curated/auction/saleroom_directory").select(
    col("saleroom_id").alias("saleroom_key"),
    col("location").alias("saleroom_location"))

# 5. Read the sale event dimension with business-qualified names
sale_event_df = spark.read.parquet("warehouse/curated/auction/sale_event_schedule").select(
    col("sale_event_id"),
    col("sale_year"),
    col("name").alias("sale_event_name"),
    col("event_timestamp").alias("sale_event_date"),
    col("saleroom_id"))

# 6. Read the lot outcome fact with business-qualified names
lot_outcome_df = spark.read.parquet("warehouse/curated/auction/lot_outcomes").select(
    col("lot_outcome_id"),
    col("sale_event_id"),
    col("bidder_id"),
    col("agency_id"),
    col("opening_slot"),
    col("best_round_id"),
    col("time").alias("hammer_time"),
    col("points").alias("score_points"),
    col("final_place"))

# 7. Resolve every sale event to the saleroom that hosted it
sale_event_saleroom_df = sale_event_df \
    .join(saleroom_df, sale_event_df.saleroom_id == saleroom_df.saleroom_key, "inner") \
    .select(sale_event_df.sale_event_id.alias("event_key"),
            col("sale_year"),
            col("sale_event_name"),
            col("sale_event_date"),
            col("saleroom_location"))

# 8. Attach event, bidder and agency context to every lot outcome
lot_sale_joined_df = lot_outcome_df \
    .join(sale_event_saleroom_df, lot_outcome_df.sale_event_id == sale_event_saleroom_df.event_key, "inner") \
    .join(bidder_df, lot_outcome_df.bidder_id == bidder_df.bidder_key, "inner") \
    .join(agency_df, lot_outcome_df.agency_id == agency_df.agency_key, "inner")

# 9. Build the published projection with the build stamp
lot_sale_results_df = lot_sale_joined_df.select(
    col("sale_year"),
    col("sale_event_name"),
    col("sale_event_date"),
    col("saleroom_location"),
    col("bidder_name"),
    col("bidder_number"),
    col("bidder_country"),
    col("agency_name"),
    col("opening_slot"),
    col("best_round_id"),
    col("hammer_time"),
    col("score_points"),
    col("final_place")) \
    .withColumn("created_timestamp", current_timestamp())

# 10. Load the rebuilt snapshot into the presentation layer
lot_sale_results_df.write \
    .mode("overwrite") \
    .parquet("warehouse/presentation/auction/lot_sale_results")

# 11. Release resources
spark.stop()
