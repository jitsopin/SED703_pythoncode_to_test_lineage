"""Business purpose: Consolidate e-commerce shipment details with logistics entities for reporting.
Source: data/curated/logistics/* (Agents, Partners, Centers, Routes, Shipments)
Target: warehouse/reporting/shipment_summaries (Parquet format)
Output grain: One record per shipment with full entity enrichment.
Business rules: Join dimensional data to fact shipments, rename columns, and append audit timestamp.
Generation profile: Portable PySpark script, standard APIs, isolated DataFrame tracking.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("ShipmentSummaryETL").getOrCreate()

# 2. Extract and standardize delivery agents
agents_df = spark.read.parquet("data/curated/logistics/delivery_agents") \
    .withColumnRenamed("badge", "agent_badge") \
    .withColumnRenamed("name", "agent_name") \
    .withColumnRenamed("region", "agent_region")

# 3. Extract and standardize shipping partners
partners_df = spark.read.parquet("data/curated/logistics/shipping_partners") \
    .withColumnRenamed("name", "partner_company")

# 4. Extract and standardize fulfillment centers
centers_df = spark.read.parquet("data/curated/logistics/fulfillment_centers") \
    .withColumnRenamed("location", "facility_location")

# 5. Extract and standardize delivery routes
routes_df = spark.read.parquet("data/curated/logistics/delivery_routes") \
    .withColumnRenamed("name", "route_name") \
    .withColumnRenamed("route_timestamp", "dispatch_date")

# 6. Extract and standardize shipment records
shipments_df = spark.read.parquet("data/curated/logistics/shipment_records") \
    .withColumnRenamed("time", "transit_time")

# 7. Join routes with fulfillment centers
route_centers_df = routes_df.join(centers_df, routes_df.center_id == centers_df.center_id, "inner") \
    .select(routes_df.route_id, routes_df.route_year, routes_df.route_name, routes_df.dispatch_date, centers_df.facility_location)

# 8. Consolidate shipment facts with logistics dimensions
shipment_details_df = shipments_df.join(route_centers_df, shipments_df.route_id == route_centers_df.route_id, "inner") \
    .join(agents_df, shipments_df.agent_id == agents_df.agent_id, "inner") \
    .join(partners_df, shipments_df.partner_id == partners_df.partner_id, "inner")

# 9. Project final reporting attributes and append timestamp
final_shipment_summary_df = shipment_details_df.select(
    col("route_year"), col("route_name"), col("dispatch_date"), col("facility_location"),
    col("agent_name"), col("agent_badge"), col("agent_region"), col("partner_company"),
    col("transit_time"), col("performance_score"), col("delivery_status")
).withColumn("report_generated_at", current_timestamp())

# 10. Persist the reporting summary
final_shipment_summary_df.write.mode("overwrite").parquet("warehouse/reporting/shipment_summaries")

# 11. Release resources
spark.stop()