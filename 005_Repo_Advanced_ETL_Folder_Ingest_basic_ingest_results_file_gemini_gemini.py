"""Business purpose: Ingest, standardize, and load e-sports match result records.
Source: data/landing/esports/match_results.json
Target: data/curated/esports/match_results_partitioned
Output grain: One record per player per match tournament.
Business rules: Rename metrics, drop server status, append timestamp, partition by tournament.
Generation profile: Flat portable PySpark script, strict DataFrame boundaries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("EsportsMatchResultsIngestion").getOrCreate()

# 2. Define schema and extract raw match results
match_schema = StructType([
    StructField("record_id", IntegerType(), False),
    StructField("tournament_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("jersey_number", IntegerType(), True),
    StructField("starting_seed", IntegerType(), True),
    StructField("final_placement", IntegerType(), True),
    StructField("placement_status", StringType(), True),
    StructField("placement_order", IntegerType(), True),
    StructField("score_points", FloatType(), True),
    StructField("rounds_survived", IntegerType(), True),
    StructField("match_duration", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("highest_kill_streak", IntegerType(), True),
    StructField("mvp_rank", IntegerType(), True),
    StructField("best_round_time", StringType(), True),
    StructField("apm_speed", FloatType(), True),
    StructField("server_status_code", StringType(), True)
])
match_raw_df = spark.read.schema(match_schema).json("data/landing/esports/match_results.json")

# 3. Standardize column naming and append ingestion timestamp
match_standardized_df = match_raw_df.withColumnRenamed("record_id", "match_record_id") \
    .withColumnRenamed("tournament_id", "event_tournament_id") \
    .withColumnRenamed("player_id", "esports_player_id") \
    .withColumnRenamed("team_id", "esports_team_id") \
    .withColumnRenamed("placement_status", "final_placement_status") \
    .withColumnRenamed("placement_order", "final_placement_order") \
    .withColumnRenamed("highest_kill_streak", "max_kill_streak") \
    .withColumnRenamed("best_round_time", "fastest_round_time") \
    .withColumnRenamed("apm_speed", "actions_per_minute") \
    .withColumn("processed_timestamp", current_timestamp())

# 4. Drop the unwanted server status column
match_final_df = match_standardized_df.drop(col("server_status_code"))

# 5. Persist curated results partitioned by tournament
match_final_df.write.mode("overwrite").partitionBy("event_tournament_id").parquet("data/curated/esports/match_results_partitioned")

# 6. Release resources
spark.stop()