"""Business purpose: Aggregate e-commerce brand performance and rank them annually.
Source: warehouse/ecommerce/daily_order_results
Target: warehouse/ecommerce/brand_annual_rankings
Output grain: One ranking record per brand per year.
Business rules: Calculate total sales, count five-star reviews, and rank brands annually by sales.
Generation profile: Flat portable PySpark script, explicit DataFrame boundaries, removed exploratory views.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, rank, sum, when
from pyspark.sql.window import Window

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("BrandPerformanceRankingETL").getOrCreate()

# 2. Extract daily order results
order_results_df = spark.read.parquet("warehouse/ecommerce/daily_order_results")

# 3. Aggregate performance metrics by year and brand
brand_performance_df = order_results_df.groupBy(
    "sales_year", "brand_name"
).agg(
    sum("order_value").alias("total_sales_value"),
    count(when(col("customer_rating") == 5, True)).alias("five_star_reviews")
)

# 4. Rank brands annually by total sales value
ranking_window = Window.partitionBy("sales_year").orderBy(desc("total_sales_value"))
brand_ranked_df = brand_performance_df.withColumn("annual_rank", rank().over(ranking_window))

# 5. Persist the ranked performance dataset
brand_ranked_df.write.mode("overwrite").parquet("warehouse/ecommerce/brand_annual_rankings")

# 6. Release resources
spark.stop()