"""Business purpose: Aggregate retail sales associate performance and rank them annually.
Source: warehouse/retail/monthly_sales_facts
Target: warehouse/retail/associate_rankings
Output grain: One ranking record per sales associate per year.
Business rules: Calculate total revenue, count top customer ratings, and rank associates within each year based on revenue and ratings.
Generation profile: Flat portable PySpark script, explicit DataFrame boundaries, excluded exploratory code.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, rank, sum, when
from pyspark.sql.window import Window

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("AssociatePerformanceRanking").getOrCreate()

# 2. Extract monthly sales facts
sales_facts_df = spark.read.parquet("warehouse/retail/monthly_sales_facts")

# 3. Aggregate performance metrics by year and associate
associate_performance_df = sales_facts_df.groupBy(
    "sales_year", "associate_name", "store_region", "store_branch"
).agg(
    sum("revenue_generated").alias("total_revenue"),
    count(when(col("customer_rating") == 5, True)).alias("top_ratings_count")
)

# 4. Rank associates annually by revenue and top ratings
ranking_window = Window.partitionBy("sales_year").orderBy(desc("total_revenue"), desc("top_ratings_count"))
associate_ranked_df = associate_performance_df.withColumn("regional_rank", rank().over(ranking_window))

# 5. Persist the ranked performance dataset
associate_ranked_df.write.mode("overwrite").parquet("warehouse/retail/associate_rankings")

# 6. Release resources
spark.stop()