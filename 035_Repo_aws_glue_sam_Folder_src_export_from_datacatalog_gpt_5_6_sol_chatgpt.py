"""
Business purpose: Export scoped metadata records into normalized namespace, object, and segment datasets with either file or JDBC delivery.
Source: JSON metadata exchange records at data/landing/metadata_exchange/catalog_records.
Targets: JSON datasets under data/curated/metadata_exchange/ or JDBC tables metadata_export.namespace_registry, object_registry, and segment_registry.
Output grain: One row per normalized metadata entity.
Important rules: Restrict processing to requested catalog scopes, split records by entity_type, project explicit schemas, and publish all three entity classes through the selected delivery mode.
Generation profile: DATAFRAME_CHAIN + METADATA_EXPORT_PORT
Runtime arguments: export_mode (files|jdbc), semicolon-separated scope names, optional JDBC URL/user/password.
Assumptions: Hidden source helper schemas and metastore-specific transformation rules are unavailable in the supplied file; only the visible entity split, schema normalization, scope consolidation, and destination-mode behavior are preserved.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType, StructField, StructType

spark = SparkSession.builder.appName("portable_metadata_export_pipeline").getOrCreate()

export_mode = sys.argv[1] if len(sys.argv) > 1 else "files"
scope_text = sys.argv[2] if len(sys.argv) > 2 else "default_scope"
scope_names = [item.strip() for item in scope_text.split(";") if item.strip()]

jdbc_url = sys.argv[3] if len(sys.argv) > 3 else "jdbc:postgresql://localhost:5432/metadata"
jdbc_user = sys.argv[4] if len(sys.argv) > 4 else "metadata_user"
jdbc_password = sys.argv[5] if len(sys.argv) > 5 else "change_me"

source_path = "data/landing/metadata_exchange/catalog_records"
namespace_target_path = "data/curated/metadata_exchange/namespace_registry"
object_target_path = "data/curated/metadata_exchange/object_registry"
segment_target_path = "data/curated/metadata_exchange/segment_registry"

namespace_target_table = "metadata_export.namespace_registry"
object_target_table = "metadata_export.object_registry"
segment_target_table = "metadata_export.segment_registry"

metadata_schema = StructType([
    StructField("catalog_scope", StringType(), True),
    StructField("entity_type", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("entity_name", StringType(), True),
    StructField("parent_id", StringType(), True),
    StructField("storage_location", StringType(), True),
    StructField("properties_json", StringType(), True),
])

metadata_raw_df = (
    spark.read
    .schema(metadata_schema)
    .json(source_path)
)

scoped_metadata_df = metadata_raw_df.filter(
    col("catalog_scope").isin(scope_names)
)

namespace_records_df = scoped_metadata_df.filter(
    trim(col("entity_type")) == "namespace"
).select(
    col("catalog_scope"),
    col("entity_id").alias("namespace_id"),
    col("entity_name").alias("namespace_name"),
    col("properties_json"),
)

object_records_df = scoped_metadata_df.filter(
    trim(col("entity_type")) == "object"
).select(
    col("catalog_scope"),
    col("entity_id").alias("object_id"),
    col("entity_name").alias("object_name"),
    col("parent_id").alias("namespace_id"),
    col("storage_location"),
    col("properties_json"),
)

segment_records_df = scoped_metadata_df.filter(
    trim(col("entity_type")) == "segment"
).select(
    col("catalog_scope"),
    col("entity_id").alias("segment_id"),
    col("entity_name").alias("segment_name"),
    col("parent_id").alias("object_id"),
    col("storage_location"),
    col("properties_json"),
)

if export_mode == "files":
    namespace_records_df.write.mode("overwrite").json(namespace_target_path)
    object_records_df.write.mode("overwrite").json(object_target_path)
    segment_records_df.write.mode("overwrite").json(segment_target_path)
elif export_mode == "jdbc":
    (
        namespace_records_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", namespace_target_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    (
        object_records_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", object_target_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    (
        segment_records_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", segment_target_table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
else:
    raise ValueError("export_mode must be 'files' or 'jdbc'")

spark.stop()
