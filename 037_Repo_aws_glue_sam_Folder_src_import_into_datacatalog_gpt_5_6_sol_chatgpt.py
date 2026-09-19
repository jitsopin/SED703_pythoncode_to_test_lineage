"""
Business purpose: Prepare portable metadata-import payloads from either normalized JSON files or a JDBC metastore source.
Sources: JSON namespace/object/segment metadata files or JDBC metastore tables, selected by runtime mode.
Targets: Three JSON catalog-exchange datasets for namespace, object, and batched segment entities.
Output grain: One catalog payload per namespace or object record, and one batched segment payload per namespace/object pair.
Important rules: Preserve two source modes, optional namespace/table prefixes for JDBC mode, optional namespace prefix for JSON mode, wrap entity items in arrays, and batch segment items by namespace/object/type before publishing.
Generation profile: DATAFRAME_CHAIN + CATALOG_IMPORT_PORT
Runtime arguments: mode (from-files|from-jdbc), base data path, optional JDBC URL/user/password, optional namespace prefix, optional object prefix.
Assumptions: The original script delegates detailed metastore normalization to an external helper module; this rewrite preserves only the visible import-shape, prefix, batching, and source-mode semantics without reproducing hidden engine-specific helper behavior.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, collect_list, concat, lit, struct
from pyspark.sql.types import (
    ArrayType,
    MapType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("portable_catalog_import_pipeline").getOrCreate()

mode = sys.argv[1] if len(sys.argv) > 1 else "from-files"
base_path = sys.argv[2] if len(sys.argv) > 2 else "data/metadata_import"
jdbc_url = sys.argv[3] if len(sys.argv) > 3 else "jdbc:postgresql://localhost:5432/metastore"
jdbc_user = sys.argv[4] if len(sys.argv) > 4 else "metadata_user"
jdbc_password = sys.argv[5] if len(sys.argv) > 5 else "change_me"
namespace_prefix = sys.argv[6] if len(sys.argv) > 6 else ""
object_prefix = sys.argv[7] if len(sys.argv) > 7 else ""

namespace_input_path = f"{base_path}/input/namespaces"
object_input_path = f"{base_path}/input/objects"
segment_input_path = f"{base_path}/input/segments"

namespace_output_path = f"{base_path}/catalog_exchange/namespaces"
object_output_path = f"{base_path}/catalog_exchange/objects"
segment_output_path = f"{base_path}/catalog_exchange/segments"

namespace_item_schema = StructType([
    StructField("description", StringType(), True),
    StructField("location_uri", StringType(), True),
    StructField("name", StringType(), False),
    StructField("parameters", MapType(StringType(), StringType(), True), True),
])

object_item_schema = StructType([
    StructField("name", StringType(), False),
    StructField("object_type", StringType(), True),
    StructField("owner", StringType(), True),
    StructField("storage_location", StringType(), True),
    StructField("parameters", MapType(StringType(), StringType(), True), True),
])

segment_item_schema = StructType([
    StructField("creation_time", StringType(), True),
    StructField("last_access_time", StringType(), True),
    StructField("namespace_name", StringType(), True),
    StructField("parameters", MapType(StringType(), StringType(), True), True),
    StructField("storage_location", StringType(), True),
    StructField("values", ArrayType(StringType(), False), False),
])

namespace_file_schema = StructType([
    StructField("type", StringType(), False),
    StructField("item", namespace_item_schema, True),
])

object_file_schema = StructType([
    StructField("database", StringType(), False),
    StructField("type", StringType(), False),
    StructField("item", object_item_schema, True),
])

segment_file_schema = StructType([
    StructField("database", StringType(), False),
    StructField("table", StringType(), False),
    StructField("type", StringType(), False),
    StructField("item", segment_item_schema, True),
])

if mode == "from-files":
    namespace_raw_df = spark.read.schema(namespace_file_schema).json(namespace_input_path)
    object_raw_df = spark.read.schema(object_file_schema).json(object_input_path)
    segment_raw_df = spark.read.schema(segment_file_schema).json(segment_input_path)

    namespace_prefixed_df = namespace_raw_df.select(
        col("type"),
        struct(
            col("item.description").alias("description"),
            col("item.location_uri").alias("location_uri"),
            concat(lit(namespace_prefix), col("item.name")).alias("name"),
            col("item.parameters").alias("parameters"),
        ).alias("item"),
    )

    object_prefixed_df = object_raw_df.select(
        concat(lit(namespace_prefix), col("database")).alias("database"),
        col("type"),
        col("item"),
    )

    segment_prefixed_df = segment_raw_df.select(
        concat(lit(namespace_prefix), col("database")).alias("database"),
        col("table"),
        col("type"),
        struct(
            col("item.creation_time").alias("creation_time"),
            col("item.last_access_time").alias("last_access_time"),
            concat(lit(namespace_prefix), col("item.namespace_name")).alias("namespace_name"),
            col("item.parameters").alias("parameters"),
            col("item.storage_location").alias("storage_location"),
            col("item.values").alias("values"),
        ).alias("item"),
    )

elif mode == "from-jdbc":
    dbs_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.DBS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    tables_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.TBLS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    partitions_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.PARTITIONS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    namespace_prefixed_df = dbs_raw_df.select(
        lit("database").alias("type"),
        struct(
            col("DESC").alias("description"),
            col("DB_LOCATION_URI").alias("location_uri"),
            concat(lit(namespace_prefix), col("NAME")).alias("name"),
            lit(None).cast(MapType(StringType(), StringType())).alias("parameters"),
        ).alias("item"),
    )

    object_prefixed_df = (
        tables_raw_df
        .join(dbs_raw_df.select("DB_ID", "NAME"), "DB_ID", "inner")
        .select(
            concat(lit(namespace_prefix), col("NAME")).alias("database"),
            lit("table").alias("type"),
            struct(
                concat(lit(object_prefix), col("TBL_NAME")).alias("name"),
                col("TBL_TYPE").alias("object_type"),
                col("OWNER").alias("owner"),
                lit(None).cast(StringType()).alias("storage_location"),
                lit(None).cast(MapType(StringType(), StringType())).alias("parameters"),
            ).alias("item"),
        )
    )

    segment_prefixed_df = (
        partitions_raw_df
        .join(tables_raw_df.select("TBL_ID", "DB_ID", "TBL_NAME"), "TBL_ID", "inner")
        .join(dbs_raw_df.select("DB_ID", "NAME"), "DB_ID", "inner")
        .select(
            concat(lit(namespace_prefix), col("NAME")).alias("database"),
            concat(lit(object_prefix), col("TBL_NAME")).alias("table"),
            lit("partition").alias("type"),
            struct(
                col("CREATE_TIME").cast("string").alias("creation_time"),
                col("LAST_ACCESS_TIME").cast("string").alias("last_access_time"),
                concat(lit(namespace_prefix), col("NAME")).alias("namespace_name"),
                lit(None).cast(MapType(StringType(), StringType())).alias("parameters"),
                lit(None).cast(StringType()).alias("storage_location"),
                array().cast(ArrayType(StringType())).alias("values"),
            ).alias("item"),
        )
    )

else:
    raise ValueError("mode must be 'from-files' or 'from-jdbc'")

namespace_payload_df = namespace_prefixed_df.select(
    col("type"),
    array(col("item")).alias("items"),
)

object_payload_df = object_prefixed_df.select(
    col("type"),
    col("database"),
    array(col("item")).alias("items"),
)

segment_payload_df = (
    segment_prefixed_df
    .groupBy("database", "table", "type")
    .agg(collect_list("item").alias("items"))
)

namespace_payload_df.write.mode("overwrite").json(namespace_output_path)
object_payload_df.write.mode("overwrite").json(object_output_path)
segment_payload_df.write.mode("overwrite").json(segment_output_path)

spark.stop()
