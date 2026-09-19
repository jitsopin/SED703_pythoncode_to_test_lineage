"""Business purpose: Calculate shipment risk scores using custom transformation logic.
Source: In-memory sequence generator (simulating shipment IDs).
Target: warehouse/supply_chain/shipment_risk_scores (CSV format)
Output grain: One record per shipment.
Business rules: Apply baseline configuration values and calculate dynamic risk scores using a UDF before consolidating to a single partition for downstream systems.
Generation profile: Portable PySpark script replacing external module dependencies.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import DoubleType

# Define standalone functions to replace external module dependencies
def get_baseline_risk_config():
    return 1.5

def calculate_shipment_risk(shipment_id):
    return float((shipment_id * 0.75) % 10.0)

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("SupplyChainShipmentRisk").getOrCreate()

# 2. Generate raw shipment sequence
shipments_raw_df = spark.range(16).toDF("shipment_id")

# 3. Inject baseline configuration values
shipments_config_df = shipments_raw_df.withColumn(
    "baseline_risk",
    lit(get_baseline_risk_config())
)

# 4. Apply custom calculation UDF
risk_udf = udf(calculate_shipment_risk, DoubleType())
shipments_scored_df = shipments_config_df.withColumn(
    "calculated_risk",
    risk_udf(shipments_config_df["shipment_id"])
)

# 5. Consolidate into a single partition and write to storage
shipments_final_df = shipments_scored_df.repartition(1)
shipments_final_df.write.mode("overwrite").option("header", "true").csv("warehouse/supply_chain/shipment_risk_scores")

# 6. Release resources
spark.stop()