"""
Business purpose: Synchronize portable metadata snapshots for jobs, catalogs, partitions, and connections from a selected source snapshot into either a serialized bundle or a destination snapshot.
Sources: JSON resource datasets under data/landing/resource_sync/live_source or a runtime serialized-input base path, plus an optional string-mapping dataset.
Targets: JSON resource datasets under a runtime serialized-output base path or data/curated/resource_sync/synchronized_target.
Output grain: One row per synchronized resource definition.
Important rules: Respect selected target types and optional resource-name filters; skip jobs without DAG metadata when requested; skip governed tables and resource links; preserve database/table/partition relationships; apply configured string substitutions to serialized resource payloads; and separate job, database, table, partition, and connection outputs.
Generation profile: DATAFRAME_CHAIN + PORTABLE_RESOURCE_SYNC
Runtime arguments: operation_mode (sync|serialize), source_mode (live|serialized), targets CSV, serialized input base, serialized output base, optional job/database/table/connection name CSV filters, skip_no_dag_jobs flag, and mapping-file path.
Assumptions: Cloud API calls, credential/profile/role handling, prompting, bucket creation, direct script copying, overwrite API checks, and provider-specific exceptions are environment orchestration rather than portable ETL; source resource definitions are represented as JSON payload strings plus the control fields needed by this script.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_contains,
    col,
    collect_list,
    expr,
    lit,
    struct,
)
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("portable_resource_snapshot_sync").getOrCreate()

operation_mode = sys.argv[1] if len(sys.argv) > 1 else "sync"
source_mode = sys.argv[2] if len(sys.argv) > 2 else "live"
targets_text = sys.argv[3] if len(sys.argv) > 3 else "job"
serialized_input_base = sys.argv[4] if len(sys.argv) > 4 else "data/landing/resource_sync/serialized_input"
serialized_output_base = sys.argv[5] if len(sys.argv) > 5 else "data/curated/resource_sync/serialized_bundle"
job_filter_text = sys.argv[6] if len(sys.argv) > 6 else ""
database_filter_text = sys.argv[7] if len(sys.argv) > 7 else ""
table_filter_text = sys.argv[8] if len(sys.argv) > 8 else ""
connection_filter_text = sys.argv[9] if len(sys.argv) > 9 else ""
skip_no_dag_jobs = (sys.argv[10].lower() == "true") if len(sys.argv) > 10 else True
mapping_path = sys.argv[11] if len(sys.argv) > 11 else "data/landing/resource_sync/mapping"

selected_targets = [item.strip() for item in targets_text.split(",") if item.strip()]
job_filter = [item.strip() for item in job_filter_text.split(",") if item.strip()]
database_filter = [item.strip() for item in database_filter_text.split(",") if item.strip()]
table_filter = [item.strip() for item in table_filter_text.split(",") if item.strip()]
connection_filter = [item.strip() for item in connection_filter_text.split(",") if item.strip()]

live_base = "data/landing/resource_sync/live_source"
source_base = serialized_input_base if source_mode == "serialized" else live_base
target_base = serialized_output_base if operation_mode == "serialize" else "data/curated/resource_sync/synchronized_target"

job_schema = StructType([
    StructField("resource_name", StringType(), False),
    StructField("payload_json", StringType(), False),
    StructField("has_dag", BooleanType(), True),
    StructField("script_location", StringType(), True),
])

database_schema = StructType([
    StructField("resource_name", StringType(), False),
    StructField("payload_json", StringType(), False),
    StructField("is_resource_link", BooleanType(), True),
])

table_schema = StructType([
    StructField("database_name", StringType(), False),
    StructField("resource_name", StringType(), False),
    StructField("payload_json", StringType(), False),
    StructField("table_type", StringType(), True),
    StructField("is_resource_link", BooleanType(), True),
])

partition_schema = StructType([
    StructField("database_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("partition_key", StringType(), False),
    StructField("payload_json", StringType(), False),
])

connection_schema = StructType([
    StructField("resource_name", StringType(), False),
    StructField("payload_json", StringType(), False),
])

mapping_schema = StructType([
    StructField("match_text", StringType(), False),
    StructField("replacement_text", StringType(), False),
])

jobs_raw_df = spark.read.schema(job_schema).json(f"{source_base}/jobs")
databases_raw_df = spark.read.schema(database_schema).json(f"{source_base}/databases")
tables_raw_df = spark.read.schema(table_schema).json(f"{source_base}/tables")
partitions_raw_df = spark.read.schema(partition_schema).json(f"{source_base}/partitions")
connections_raw_df = spark.read.schema(connection_schema).json(f"{source_base}/connections")
mapping_raw_df = spark.read.schema(mapping_schema).json(mapping_path)

mapping_pairs_df = mapping_raw_df.select(
    struct(
        col("match_text"),
        col("replacement_text"),
    ).alias("mapping_pair")
).agg(
    collect_list("mapping_pair").alias("mapping_pairs")
)

jobs_selected_df = jobs_raw_df.filter(
    lit("job").isin(selected_targets)
).filter(
    (lit(len(job_filter)) == 0) | col("resource_name").isin(job_filter)
).filter(
    (~lit(skip_no_dag_jobs)) | (col("has_dag") == lit(True))
)

databases_selected_df = databases_raw_df.filter(
    lit("catalog").isin(selected_targets)
).filter(
    (lit(len(database_filter)) == 0) | col("resource_name").isin(database_filter)
).filter(
    ~col("is_resource_link")
)

tables_selected_df = tables_raw_df.filter(
    lit("catalog").isin(selected_targets)
).filter(
    (lit(len(database_filter)) == 0) | col("database_name").isin(database_filter)
).filter(
    (lit(len(table_filter)) == 0) | col("resource_name").isin(table_filter)
).filter(
    (col("table_type") != lit("GOVERNED")) | col("table_type").isNull()
).filter(
    ~col("is_resource_link")
)

partitions_selected_df = partitions_raw_df.join(
    tables_selected_df.select(
        col("database_name").alias("selected_database_name"),
        col("resource_name").alias("selected_table_name"),
    ),
    (partitions_raw_df.database_name == col("selected_database_name"))
    & (partitions_raw_df.table_name == col("selected_table_name")),
    "inner",
).select(
    partitions_raw_df.database_name,
    partitions_raw_df.table_name,
    partitions_raw_df.partition_key,
    partitions_raw_df.payload_json,
)

connections_selected_df = connections_raw_df.filter(
    lit("connection").isin(selected_targets)
).filter(
    (lit(len(connection_filter)) == 0) | col("resource_name").isin(connection_filter)
)

jobs_mapped_df = jobs_selected_df.crossJoin(mapping_pairs_df).select(
    col("resource_name"),
    expr(
        "aggregate(mapping_pairs, payload_json, "
        "(acc, x) -> replace(acc, x.match_text, x.replacement_text))"
    ).alias("payload_json"),
    col("has_dag"),
    col("script_location"),
)

databases_mapped_df = databases_selected_df.crossJoin(mapping_pairs_df).select(
    col("resource_name"),
    expr(
        "aggregate(mapping_pairs, payload_json, "
        "(acc, x) -> replace(acc, x.match_text, x.replacement_text))"
    ).alias("payload_json"),
)

tables_mapped_df = tables_selected_df.crossJoin(mapping_pairs_df).select(
    col("database_name"),
    col("resource_name"),
    expr(
        "aggregate(mapping_pairs, payload_json, "
        "(acc, x) -> replace(acc, x.match_text, x.replacement_text))"
    ).alias("payload_json"),
    col("table_type"),
)

partitions_mapped_df = partitions_selected_df.crossJoin(mapping_pairs_df).select(
    col("database_name"),
    col("table_name"),
    col("partition_key"),
    expr(
        "aggregate(mapping_pairs, payload_json, "
        "(acc, x) -> replace(acc, x.match_text, x.replacement_text))"
    ).alias("payload_json"),
)

connections_mapped_df = connections_selected_df.crossJoin(mapping_pairs_df).select(
    col("resource_name"),
    expr(
        "aggregate(mapping_pairs, payload_json, "
        "(acc, x) -> replace(acc, x.match_text, x.replacement_text))"
    ).alias("payload_json"),
)

jobs_mapped_df.write.mode("overwrite").json(f"{target_base}/jobs")
databases_mapped_df.write.mode("overwrite").json(f"{target_base}/databases")
tables_mapped_df.write.mode("overwrite").json(f"{target_base}/tables")
partitions_mapped_df.write.mode("overwrite").json(f"{target_base}/partitions")
connections_mapped_df.write.mode("overwrite").json(f"{target_base}/connections")

spark.stop()
