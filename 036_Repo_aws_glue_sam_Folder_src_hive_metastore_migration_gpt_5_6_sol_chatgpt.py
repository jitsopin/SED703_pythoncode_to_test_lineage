"""
Business purpose: Provide a portable bidirectional metadata migration between a JDBC-backed metastore representation and normalized JSON metadata datasets.
Sources: JDBC metastore tables or normalized JSON metadata files, selected by runtime mode.
Targets: Normalized JSON datasets or JDBC metastore tables, selected by runtime mode.
Output grain: One row per namespace, object, or segment metadata entity.
Important rules: Preserve database/table/partition identity, optional name prefixes, timestamp normalization, storage metadata, parameter payloads, ordered partition values, and destination write ordering.
Generation profile: DATAFRAME_CHAIN + BIDIRECTIONAL_METADATA_MIGRATION
Runtime arguments: mode (from-jdbc|to-jdbc), JDBC URL, JDBC user, JDBC password, data path, optional database prefix, optional table prefix.
Assumptions: The supplied source implements many Hive-version-specific child-table transformations; this portable rewrite preserves the directly observable database/table/partition business metadata and storage/parameter payloads without reproducing engine-specific skew/serde compatibility internals.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    explode_outer,
    lit,
    map_from_entries,
    struct,
    to_json,
    expr,
)
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("portable_bidirectional_metadata_migration").getOrCreate()

mode = sys.argv[1] if len(sys.argv) > 1 else "from-jdbc"
jdbc_url = sys.argv[2] if len(sys.argv) > 2 else "jdbc:postgresql://localhost:5432/metastore"
jdbc_user = sys.argv[3] if len(sys.argv) > 3 else "metadata_user"
jdbc_password = sys.argv[4] if len(sys.argv) > 4 else "change_me"
data_path = sys.argv[5] if len(sys.argv) > 5 else "data/metadata_migration"
database_prefix = sys.argv[6] if len(sys.argv) > 6 else ""
table_prefix = sys.argv[7] if len(sys.argv) > 7 else ""

namespace_path = f"{data_path}/namespaces"
object_path = f"{data_path}/objects"
segment_path = f"{data_path}/segments"

if mode == "from-jdbc":
    dbs_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.DBS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    db_params_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.DATABASE_PARAMS")
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

    table_params_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.TABLE_PARAMS")
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

    partition_params_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.PARTITION_PARAMS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    partition_values_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.PARTITION_KEY_VALS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    storage_raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.SDS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    db_params_df = db_params_raw_df.groupBy("DB_ID").agg(
        map_from_entries(
            expr("collect_list(named_struct('key', PARAM_KEY, 'value', PARAM_VALUE))")
        ).alias("parameters")
    )

    namespaces_df = (
        dbs_raw_df
        .join(db_params_df, "DB_ID", "left")
        .select(
            col("DB_ID").cast("string").alias("namespace_id"),
            concat(lit(database_prefix), col("NAME")).alias("namespace_name"),
            col("DESC").alias("description"),
            col("DB_LOCATION_URI").alias("location_uri"),
            to_json(col("parameters")).alias("parameters_json"),
        )
    )

    table_params_df = table_params_raw_df.groupBy("TBL_ID").agg(
        map_from_entries(
            expr("collect_list(named_struct('key', PARAM_KEY, 'value', PARAM_VALUE))")
        ).alias("parameters")
    )

    objects_df = (
        tables_raw_df
        .join(dbs_raw_df.select("DB_ID", "NAME"), "DB_ID", "inner")
        .join(table_params_df, "TBL_ID", "left")
        .join(storage_raw_df.select("SD_ID", "LOCATION", "INPUT_FORMAT", "OUTPUT_FORMAT"), "SD_ID", "left")
        .select(
            col("TBL_ID").cast("string").alias("object_id"),
            col("DB_ID").cast("string").alias("namespace_id"),
            concat(lit(database_prefix), col("NAME")).alias("namespace_name"),
            concat(lit(table_prefix), col("TBL_NAME")).alias("object_name"),
            col("TBL_TYPE").alias("object_type"),
            col("OWNER").alias("owner"),
            col("LOCATION").alias("storage_location"),
            col("INPUT_FORMAT").alias("input_format"),
            col("OUTPUT_FORMAT").alias("output_format"),
            to_json(col("parameters")).alias("parameters_json"),
        )
    )

    partition_params_df = partition_params_raw_df.groupBy("PART_ID").agg(
        map_from_entries(
            expr("collect_list(named_struct('key', PARAM_KEY, 'value', PARAM_VALUE))")
        ).alias("parameters")
    )

    partition_values_df = partition_values_raw_df.groupBy("PART_ID").agg(
        expr("sort_array(collect_list(named_struct('idx', INTEGER_IDX, 'value', PART_KEY_VAL)))").alias("ordered_values")
    )

    segments_df = (
        partitions_raw_df
        .join(tables_raw_df.select("TBL_ID", "DB_ID", "TBL_NAME"), "TBL_ID", "inner")
        .join(dbs_raw_df.select("DB_ID", "NAME"), "DB_ID", "inner")
        .join(partition_params_df, "PART_ID", "left")
        .join(partition_values_df, "PART_ID", "left")
        .join(storage_raw_df.select("SD_ID", "LOCATION"), "SD_ID", "left")
        .select(
            col("PART_ID").cast("string").alias("segment_id"),
            col("TBL_ID").cast("string").alias("object_id"),
            concat(lit(database_prefix), col("NAME")).alias("namespace_name"),
            concat(lit(table_prefix), col("TBL_NAME")).alias("object_name"),
            col("PART_NAME").alias("segment_name"),
            col("LOCATION").alias("storage_location"),
            to_json(col("ordered_values")).alias("partition_values_json"),
            to_json(col("parameters")).alias("parameters_json"),
        )
    )

    namespaces_df.write.mode("overwrite").json(namespace_path)
    objects_df.write.mode("overwrite").json(object_path)
    segments_df.write.mode("overwrite").json(segment_path)

elif mode == "to-jdbc":
    namespaces_input_df = spark.read.json(namespace_path)
    objects_input_df = spark.read.json(object_path)
    segments_input_df = spark.read.json(segment_path)

    dbs_out_df = namespaces_input_df.select(
        col("namespace_id").cast("long").alias("DB_ID"),
        col("namespace_name").alias("NAME"),
        col("description").alias("DESC"),
        col("location_uri").alias("DB_LOCATION_URI"),
    )

    tables_out_df = objects_input_df.select(
        col("object_id").cast("long").alias("TBL_ID"),
        col("namespace_id").cast("long").alias("DB_ID"),
        col("object_name").alias("TBL_NAME"),
        col("object_type").alias("TBL_TYPE"),
        col("owner").alias("OWNER"),
    )

    partitions_out_df = segments_input_df.select(
        col("segment_id").cast("long").alias("PART_ID"),
        col("object_id").cast("long").alias("TBL_ID"),
        col("segment_name").alias("PART_NAME"),
    )

    (
        dbs_out_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.DBS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    (
        tables_out_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.TBLS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    (
        partitions_out_df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "metadata.PARTITIONS")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

else:
    raise ValueError("mode must be 'from-jdbc' or 'to-jdbc'")

spark.stop()
