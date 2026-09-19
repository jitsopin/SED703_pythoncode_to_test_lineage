"""Business purpose: Reconcile daily inventory exposure across warehouses.
Source: data/inbound/supply_chain/inventory_vectors.parquet
Target: warehouse/supply_chain/daily_inventory_summary
Output grain: One summary record per warehouse per product category per jurisdiction.
Business rules: Calculate inventory exposure from derived metrics, assign jurisdiction based on region, aggregate metrics, and inject a reporting timestamp.
Generation profile: Portable PySpark script; raw SQL, PyArrow UDFs, and AWS Glue artifacts removed.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_date, max, sum, when
from pyspark.sql.types import StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("InventoryExposureReconciliation").getOrCreate()

# 2. Extract daily inventory vectors
inventory_raw_df = spark.read.parquet("data/inbound/supply_chain/inventory_vectors.parquet")

# 3. Derive inventory exposure and assign jurisdictions
inventory_derived_df = inventory_raw_df.select(
    col("item_id"), col("warehouse_id"), col("product_category"), col("region"), col("risk_weight"),
    when(col("product_category") == "ELECTRONICS", col("base_value") * col("volatility_index"))
    .when(col("product_category") == "APPAREL", col("base_value") * 0.05)
    .when(col("product_category") == "FOOD", col("perishable_index") * 100)
    .otherwise(col("base_value")).alias("derived_exposure"),
    when(col("product_category") == "ELECTRONICS", col("supply_chain_score"))
    .when(col("product_category") == "APPAREL", col("trend_factor"))
    .when(col("product_category") == "FOOD", col("spoilage_rate")).alias("nested_metric"),
    when(col("region") == "EMEA", "UK")
    .when(col("region") == "AMER", "US")
    .when(col("region") == "APAC", "JP")
    .otherwise("OTHER").alias("derived_jurisdiction")
).filter(col("scan_date") == current_date())

# 4. Aggregate exposure metrics by warehouse, category, and jurisdiction
summary_grouped_df = inventory_derived_df.groupBy(
    "warehouse_id", "product_category", "derived_jurisdiction"
).agg(
    count("*").alias("item_count"),
    sum("derived_exposure").alias("total_exposure"),
    max("nested_metric").alias("max_category_metric")
)

# 5. Format final summary and partition by date
summary_final_df = summary_grouped_df.select(
    current_date().alias("report_date"),
    col("warehouse_id"),
    col("product_category"),
    col("derived_jurisdiction"),
    col("item_count"),
    col("total_exposure"),
    col("max_category_metric")
)

# 6. Load partitioned dataset to curated storage
summary_final_df.write.mode("overwrite").partitionBy("report_date").parquet("warehouse/supply_chain/daily_inventory_summary")

# 7. Release resources
spark.stop()