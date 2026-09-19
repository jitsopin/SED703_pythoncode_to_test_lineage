"""Business purpose: Ingest and consolidate SAP purchase order tracking logs.
Source: data/landing/sap/purchase_orders.json
Target: warehouse/sap/purchase_orders_archived (JSON format)
Output grain: One record per purchase order event.
Business rules: Extract raw JSON logs, consolidate into a single physical partition to resolve small-file issues, and persist to the archive.
Generation profile: Portable PySpark script replacing AWS Glue bookmark logic and proprietary dynamic frames.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("SAPPurchaseOrderArchival").getOrCreate()

# 2. Extract raw purchase order JSON data
po_schema = StructType([
    StructField("po_number", StringType(), False),
    StructField("vendor_id", StringType(), True),
    StructField("material_code", StringType(), True),
    StructField("order_quantity", IntegerType(), True)
])
po_raw_df = spark.read.schema(po_schema).json("data/landing/sap/purchase_orders.json")

# 3. Consolidate small files into a single partition
po_consolidated_df = po_raw_df.repartition(1)

# 4. Load consolidated records to the archive
po_consolidated_df.write.mode("overwrite").json("warehouse/sap/purchase_orders_archived")

# 5. Release resources
spark.stop()