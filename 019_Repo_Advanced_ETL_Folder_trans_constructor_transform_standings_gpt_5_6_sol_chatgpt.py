"""
Business purpose: Build a portable annual team standings dataset from presentation-level event results.
Source: Parquet event-result presentation data at data/presentation/competition/published_event_results.
Target: Parquet standings dataset at data/presentation/competition/annual_team_standings.
Output grain: One row per season_year and team_name.
Important rules: Aggregate team points and wins, rank teams within each season by total points, and overwrite the final standings snapshot.
Generation profile: DATAFRAME_CHAIN + TEAM_STANDINGS_FLOW
Runtime arguments: None.
Assumptions: finish_position = 1 represents a win; exploratory temporary-view and Delta branches are not part of the required business output.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank, sum, when
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_team_standings_pipeline").getOrCreate()

source_path = "data/presentation/competition/published_event_results"
target_path = "data/presentation/competition/annual_team_standings"

# 2. Extract
event_result_df = spark.read.parquet(source_path).select(
    col("season_year"),
    col("team_name"),
    col("points_awarded"),
    col("finish_position"),
)

# 3. Aggregate team standings measures
team_standings_df = (
    event_result_df
    .groupBy(
        "season_year",
        "team_name",
    )
    .agg(
        sum("points_awarded").alias("total_points"),
        count(when(col("finish_position") == 1, True)).alias("wins"),
    )
)

# 4. Rank teams within each season
team_rank_spec = Window.partitionBy("season_year").orderBy(
    col("total_points").desc(),
)

ranked_team_standings_df = team_standings_df.withColumn(
    "standing_rank",
    rank().over(team_rank_spec),
)

# 5. Load
ranked_team_standings_df.write.mode("overwrite").parquet(target_path)

# 6. Release resources
spark.stop()
