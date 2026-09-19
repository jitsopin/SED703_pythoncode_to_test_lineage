"""
Business purpose: load an exported catalog back into the target catalog, batching
the records the way the loader expects and optionally renaming every namespace
with a prefix so an import can sit beside what is already there.
Sources: warehouse/export/catalog/namespaces, datasets and segments, the three
JSON exports of a catalog.
Targets: the tables catalog_import.namespace_batch, dataset_batch and
segment_batch, each replaced on every run.
Output grain: one row per namespace, one per dataset, and one per namespace,
dataset and record type for segments, each carrying its records in a list.
Business rules: the namespace prefix is applied to the namespace name of every
record so the three exports stay consistent with each other, each record is
carried in a list even when it is alone, and segments of the same dataset are
gathered into one list so they load together.
Generation profile: portable flat batch PySpark catalog import.
Runtime arguments: argv[1] is the namespace prefix; it falls back to an empty
string, which leaves every name unchanged.
Assumptions: the source file is a migration library driven by argument parsing, so
only its import-from-export path is rebuilt here as one flat script; its other
mode re-ran the metastore extract and transform that the export pipeline already
performs, so it is not repeated; the catalog is written as tables because the
catalog API sink of the original is not portable; the prefix is applied by
concatenation with an empty default rather than a branch; and the original rebuilt
the segment record with its creation time listed twice and its last access time
dropped, which is not reproduced here.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, collect_list, concat, lit, struct
from pyspark.sql.types import (IntegerType, MapType, StringType, StructField,
                               StructType)

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("catalog_import_from_export").getOrCreate()
namespace_prefix = sys.argv[1] if len(sys.argv) > 1 else ""

# 2. Read the namespace export with an explicit schema contract
namespace_export_schema = StructType(fields=[
    StructField("item", StructType(fields=[
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("location_uri", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]), True),
    StructField("type", StringType(), True),
])

namespace_export_df = spark.read \
    .schema(namespace_export_schema) \
    .json("warehouse/export/catalog/namespaces")

# 3. Rename every namespace with the prefix
namespace_prefixed_df = namespace_export_df.select(
    struct(concat(lit(namespace_prefix), col("item.name")).alias("name"),
           col("item.description").alias("description"),
           col("item.location_uri").alias("location_uri"),
           col("item.properties").alias("properties")).alias("item"),
    col("type"))

# 4. Carry every namespace record in a list
namespace_batch_df = namespace_prefixed_df.select(
    col("type"),
    array(col("item")).alias("items"))

# 5. Load the namespaces
namespace_batch_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("catalog_import.namespace_batch")

# 6. Read the dataset export with an explicit schema contract
dataset_export_schema = StructType(fields=[
    StructField("namespace_name", StringType(), True),
    StructField("item", StructType(fields=[
        StructField("name", StringType(), True),
        StructField("dataset_type", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("last_access_at", StringType(), True),
        StructField("retention_days", IntegerType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]), True),
    StructField("type", StringType(), True),
])

dataset_export_df = spark.read \
    .schema(dataset_export_schema) \
    .json("warehouse/export/catalog/datasets")

# 7. Rename the owning namespace with the prefix
dataset_prefixed_df = dataset_export_df.select(
    concat(lit(namespace_prefix), col("namespace_name")).alias("namespace_name"),
    col("item"),
    col("type"))

# 8. Carry every dataset record in a list
dataset_batch_df = dataset_prefixed_df.select(
    col("type"),
    col("namespace_name"),
    array(col("item")).alias("items"))

# 9. Load the datasets
dataset_batch_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("catalog_import.dataset_batch")

# 10. Read the segment export with an explicit schema contract
segment_export_schema = StructType(fields=[
    StructField("namespace_name", StringType(), True),
    StructField("dataset_name", StringType(), True),
    StructField("item", StructType(fields=[
        StructField("segment_name", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("last_access_at", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]), True),
    StructField("type", StringType(), True),
])

segment_export_df = spark.read \
    .schema(segment_export_schema) \
    .json("warehouse/export/catalog/segments")

# 11. Rename the owning namespace with the prefix
segment_prefixed_df = segment_export_df.select(
    concat(lit(namespace_prefix), col("namespace_name")).alias("namespace_name"),
    col("dataset_name"),
    col("item"),
    col("type"))

# 12. Gather the segments of one dataset into a single list
segment_batch_df = segment_prefixed_df \
    .groupBy("namespace_name", "dataset_name", "type") \
    .agg(collect_list(col("item")).alias("items"))

# 13. Load the segments
segment_batch_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("catalog_import.segment_batch")

# 14. Release resources
spark.stop()
