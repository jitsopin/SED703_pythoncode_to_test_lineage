"""
Business purpose: Build a portable annual participant standings dataset from presentation-level event results.
Source: Parquet event-result presentation data at data/presentation/competition/published_event_results.
Target: Parquet standings dataset at data/presentation/competition/annual_participant_standings.
Output grain: One row per season_year, participant_name, participant_nationality, and team_name.
Important rules: Aggregate points and wins, rank participants within each season by total points then wins, and overwrite the final standings snapshot.
Generation profile: DATAFRAME_CHAIN + WINDOW_STANDINGS_FLOW
Runtime arguments: None.
Assumptions: finish_position = 1 represents a win and the source contains one row per participant result within an event.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank, sum, when
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_participant_standings_pipeline").getOrCreate()

source_path = "data/presentation/competition/published_event_results"
target_path = "data/presentation/competition/annual_participant_standings"

# 2. Extract
event_result_df = spark.read.parquet(source_path).select(
    col("season_year"),
    col("participant_name"),
    col("participant_nationality"),
    col("team_name"),
    col("points_awarded"),
    col("finish_position"),
)

# 3. Aggregate standings measures
participant_standings_df = (
    event_result_df
    .groupBy(
        "season_year",
        "participant_name",
        "participant_nationality",
        "team_name",
    )
    .agg(
        sum("points_awarded").alias("total_points"),
        count(when(col("finish_position") == 1, True)).alias("wins"),
    )
)

# 4. Rank participants within each season
participant_rank_spec = Window.partitionBy("season_year").orderBy(
    col("total_points").desc(),
    col("wins").desc(),
)

ranked_participant_standings_df = participant_standings_df.withColumn(
    "standing_rank",
    rank().over(participant_rank_spec),
)

# 5. Load
ranked_participant_standings_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
