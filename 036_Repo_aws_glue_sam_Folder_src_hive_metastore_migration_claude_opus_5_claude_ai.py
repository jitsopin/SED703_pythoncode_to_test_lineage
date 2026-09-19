"""
Business purpose: export the contents of the legacy catalog database as three JSON
documents, so the catalog can be inspected and reloaded elsewhere without querying
the operational metastore.
Sources: the tables namespace_registry, namespace_property, dataset_registry,
dataset_property, dataset_segment and segment_property of the legacy catalog
database, read over JDBC.
Targets: warehouse/export/catalog/namespaces, datasets and segments, each written
as JSON and replaced on every run.
Output grain: one row per namespace, one per dataset and one per dataset segment.
Business rules: the property tables hold one key and value per row and are folded
into a single map per owning record, epoch seconds are published as timestamps,
internal surrogate keys and ownership columns are not exported, a record without a
name is not exported, and every exported record carries its attributes in one
nested object beside the record type.
Generation profile: portable flat batch PySpark catalog export.
Runtime arguments: argv[1] is the JDBC url of the legacy catalog database, argv[2]
the user and argv[3] the password; they fall back to a local database, "catalog"
and an empty password.
Assumptions: the source file is a migration library built from classes and
functions, so only its export path is rebuilt here as one flat script; the storage
descriptor assembly, the skewed, bucketing, sort and serde attributes and the
segment value lists are left out, which is why segments are not filtered on their
value list as the original did; and the reverse path that writes back into a
metastore is not part of this pipeline.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, from_unixtime, lit, map_from_entries, struct

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("legacy_catalog_export").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/legacy_catalog"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "catalog"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the namespace registry
namespace_registry_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.namespace_registry") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("namespace_id"),
            col("namespace_name"),
            col("namespace_description"),
            col("namespace_location"))

# 3. Read the namespace properties, one key and value per row
namespace_property_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.namespace_property") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("namespace_id"), col("property_key"), col("property_value"))

# 4. Fold the namespace properties into one map per namespace
namespace_property_map_df = namespace_property_df \
    .groupBy("namespace_id") \
    .agg(map_from_entries(collect_list(struct(col("property_key"), col("property_value"))))
         .alias("properties"))

# 5. Attach the properties and publish the namespace attributes
namespace_attributes_df = namespace_registry_df \
    .join(namespace_property_map_df, "namespace_id", "left_outer") \
    .select(col("namespace_name").alias("name"),
            col("namespace_description").alias("description"),
            col("namespace_location").alias("location_uri"),
            col("properties")) \
    .filter(col("name").isNotNull())

# 6. Carry the attributes in one nested object beside the record type
namespace_item_df = namespace_attributes_df.select(
    struct(col("name"),
           col("description"),
           col("location_uri"),
           col("properties")).alias("item"),
    lit("namespace").alias("type"))

# 7. Export the namespaces
namespace_item_df.write \
    .mode("overwrite") \
    .json("warehouse/export/catalog/namespaces")

# 8. Read the dataset registry
dataset_registry_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.dataset_registry") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("dataset_id"),
            col("namespace_id"),
            col("dataset_name"),
            col("dataset_type"),
            col("created_epoch"),
            col("last_access_epoch"),
            col("retention_days"))

# 9. Resolve every dataset to the namespace that holds it
dataset_named_df = dataset_registry_df.join(
    namespace_registry_df.select(col("namespace_id"), col("namespace_name")),
    "namespace_id",
    "inner")

# 10. Read the dataset properties, one key and value per row
dataset_property_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.dataset_property") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("dataset_id"), col("property_key"), col("property_value"))

# 11. Fold the dataset properties into one map per dataset
dataset_property_map_df = dataset_property_df \
    .groupBy("dataset_id") \
    .agg(map_from_entries(collect_list(struct(col("property_key"), col("property_value"))))
         .alias("properties"))

# 12. Attach the properties and publish the dataset attributes
dataset_attributes_df = dataset_named_df \
    .join(dataset_property_map_df, "dataset_id", "left_outer") \
    .select(col("namespace_name"),
            col("dataset_name").alias("name"),
            col("dataset_type"),
            from_unixtime(col("created_epoch")).alias("created_at"),
            from_unixtime(col("last_access_epoch")).alias("last_access_at"),
            col("retention_days"),
            col("properties")) \
    .filter(col("name").isNotNull() & col("namespace_name").isNotNull())

# 13. Carry the attributes in one nested object beside the namespace and the type
dataset_item_df = dataset_attributes_df.select(
    col("namespace_name"),
    struct(col("name"),
           col("dataset_type"),
           col("created_at"),
           col("last_access_at"),
           col("retention_days"),
           col("properties")).alias("item"),
    lit("dataset").alias("type"))

# 14. Export the datasets
dataset_item_df.write \
    .mode("overwrite") \
    .json("warehouse/export/catalog/datasets")

# 15. Read the dataset segments
segment_registry_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.dataset_segment") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("segment_id"),
            col("dataset_id"),
            col("segment_name"),
            col("created_epoch"),
            col("last_access_epoch"))

# 16. Resolve every segment to its dataset and namespace
segment_named_df = segment_registry_df.join(
    dataset_named_df.select(col("dataset_id"), col("dataset_name"), col("namespace_name")),
    "dataset_id",
    "inner")

# 17. Read the segment properties, one key and value per row
segment_property_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "legacy_catalog.segment_property") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select(col("segment_id"), col("property_key"), col("property_value"))

# 18. Fold the segment properties into one map per segment
segment_property_map_df = segment_property_df \
    .groupBy("segment_id") \
    .agg(map_from_entries(collect_list(struct(col("property_key"), col("property_value"))))
         .alias("properties"))

# 19. Attach the properties and publish the segment attributes
segment_attributes_df = segment_named_df \
    .join(segment_property_map_df, "segment_id", "left_outer") \
    .select(col("namespace_name"),
            col("dataset_name"),
            col("segment_name"),
            from_unixtime(col("created_epoch")).alias("created_at"),
            from_unixtime(col("last_access_epoch")).alias("last_access_at"),
            col("properties")) \
    .filter(col("segment_name").isNotNull())

# 20. Carry the attributes in one nested object beside the owning names
segment_item_df = segment_attributes_df.select(
    col("namespace_name"),
    col("dataset_name"),
    struct(col("segment_name"),
           col("created_at"),
           col("last_access_at"),
           col("properties")).alias("item"),
    lit("segment").alias("type"))

# 21. Export the segments
segment_item_df.write \
    .mode("overwrite") \
    .json("warehouse/export/catalog/segments")

# 22. Release resources
spark.stop()
