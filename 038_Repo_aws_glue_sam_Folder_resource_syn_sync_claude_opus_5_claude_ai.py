"""
Business purpose: work out what has to change in the target environment before
anything is touched there, so an operator can review the plan and downstream tools
can apply creates and updates separately.
Sources: the table platform_curated.source_resource_inventory holding what exists
in the source environment, the table platform_curated.target_resource_inventory
holding what the target already has, and
data/landing/platform/environment_mapping.csv giving the storage prefix each
region uses in the target.
Target: the table platform_curated.resource_sync_plan, partitioned by sync_action
and replaced on every run.
Output grain: one row per resource that needs work, that is one row per
resource_id with a sync_action other than unchanged.
Business rules: a resource whose region has no mapping is not planned, the target
location is the region's target prefix followed by the resource's relative path, a
resource the target does not hold yet is a create, a resource the source has
changed more recently than the target is an update, anything else is unchanged and
is left out of the plan, and every plan records when it was drawn up.
Generation profile: portable flat batch PySpark reconciliation.
Runtime arguments: none.
Assumptions: the source file synchronizes cloud resources through a control plane
API and holds no data flow at all, so this pipeline is not extracted from it; the
datasets, the columns and the rules above are constructed from its idea of
comparing two environments and deciding between a create and an update, and none
of them appear in the original.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit, when
from pyspark.sql.types import StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("platform_resource_sync_plan").getOrCreate()

# 2. Read what the source environment holds
source_resource_df = spark.table("platform_curated.source_resource_inventory").select(
    col("resource_id"),
    col("resource_type"),
    col("resource_name"),
    col("region_code"),
    col("relative_path"),
    col("config_json"),
    col("updated_epoch"))

# 3. Read the storage prefix each region uses in the target
environment_mapping_schema = StructType(fields=[
    StructField("region_code", StringType(), False),
    StructField("target_prefix", StringType(), True),
])

environment_mapping_df = spark.read \
    .option("header", True) \
    .schema(environment_mapping_schema) \
    .csv("data/landing/platform/environment_mapping.csv")

# 4. Resolve every resource to the location it will occupy in the target
resource_mapped_df = source_resource_df \
    .join(environment_mapping_df, "region_code", "inner") \
    .select(col("resource_id"),
            col("resource_type"),
            col("resource_name"),
            col("region_code"),
            concat(col("target_prefix"), lit("/"), col("relative_path")).alias("target_location"),
            col("config_json"),
            col("updated_epoch"))

# 5. Read what the target environment already holds
target_resource_df = spark.table("platform_curated.target_resource_inventory").select(
    col("resource_id").alias("target_resource_id"),
    col("updated_epoch").alias("target_updated_epoch"))

# 6. Line every source resource up against the target
resource_compared_df = resource_mapped_df.join(
    target_resource_df,
    resource_mapped_df.resource_id == target_resource_df.target_resource_id,
    "left_outer")

# 7. Decide what has to happen to each resource
resource_planned_df = resource_compared_df.withColumn(
    "sync_action",
    when(col("target_resource_id").isNull(), lit("create"))
    .when(col("updated_epoch") > col("target_updated_epoch"), lit("update"))
    .otherwise(lit("unchanged")))

# 8. Keep only the resources that need work
resource_pending_df = resource_planned_df \
    .filter(col("sync_action") != "unchanged")

# 9. Build the plan with the time it was drawn up
resource_sync_plan_df = resource_pending_df \
    .withColumn("planned_timestamp", current_timestamp()) \
    .select(col("resource_id"),
            col("resource_type"),
            col("resource_name"),
            col("region_code"),
            col("target_location"),
            col("config_json"),
            col("updated_epoch"),
            col("target_updated_epoch"),
            col("planned_timestamp"),
            col("sync_action"))

# 10. Replace the plan, keeping creates and updates apart
resource_sync_plan_df.write \
    .mode("overwrite") \
    .partitionBy("sync_action") \
    .saveAsTable("platform_curated.resource_sync_plan")

# 11. Release resources
spark.stop()
