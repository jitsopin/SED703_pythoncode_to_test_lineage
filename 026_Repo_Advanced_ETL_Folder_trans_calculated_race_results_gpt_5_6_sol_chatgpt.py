"""
Business purpose: Build a portable presentation table of calculated event-result points with incremental upsert semantics.
Sources: Curated Parquet result, participant, organization, and event datasets under data/curated/calculated_results/.
Target: Managed Parquet table analytics_presentation.calculated_event_results.
Output grain: One row per participant_id and event_id.
Important rules: Filter result facts to finish_position <= 10 and the requested file_date, calculate 11 - finish_position, join participant/team/event context, preserve existing dimensional values on matches, update measures and updated timestamp on matches, insert new rows with created timestamp, and overwrite the reconciled target.
Generation profile: DATAFRAME_CHAIN + PORTABLE_UPSERT_FLOW
Runtime arguments: Optional first command-line argument supplies file_date; defaults to "2021-03-21".
Assumptions: A Spark catalog supporting tableExists, table reads, and saveAsTable is available.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_calculated_event_result_pipeline").getOrCreate()

file_date = sys.argv[1] if len(sys.argv) > 1 else "2021-03-21"

result_path = "data/curated/calculated_results/result_facts"
participant_path = "data/curated/calculated_results/participant_reference"
organization_path = "data/curated/calculated_results/organization_reference"
event_path = "data/curated/calculated_results/event_reference"
target_table = "analytics_presentation.calculated_event_results"

target_schema = StructType([
    StructField("season_year", IntegerType(), True),
    StructField("organization_name", StringType(), True),
    StructField("participant_id", IntegerType(), False),
    StructField("participant_name", StringType(), True),
    StructField("event_id", IntegerType(), False),
    StructField("finish_position", IntegerType(), True),
    StructField("awarded_points", IntegerType(), True),
    StructField("calculated_points", IntegerType(), True),
    StructField("created_at_utc", TimestampType(), True),
    StructField("updated_at_utc", TimestampType(), True),
])

# 2. Read result facts
result_fact_df = spark.read.parquet(result_path).select(
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("finish_position"),
    col("awarded_points"),
    col("file_date"),
)

# 3. Filter incremental result rows and calculate points
eligible_result_df = result_fact_df.filter(
    (col("finish_position") <= 10) &
    (col("file_date") == lit(file_date))
).select(
    col("event_id"),
    col("participant_id"),
    col("organization_id"),
    col("finish_position"),
    col("awarded_points"),
    (lit(11) - col("finish_position")).alias("calculated_points"),
)

# 4. Read participant reference
participant_reference_df = spark.read.parquet(participant_path).select(
    col("participant_id"),
    col("participant_name"),
)

# 5. Read organization reference
organization_reference_df = spark.read.parquet(organization_path).select(
    col("organization_id"),
    col("organization_name"),
)

# 6. Read event reference
event_reference_df = spark.read.parquet(event_path).select(
    col("event_id"),
    col("season_year"),
)

# 7. Join incremental update rows
incoming_event_result_df = (
    eligible_result_df
    .join(
        participant_reference_df,
        eligible_result_df.participant_id == participant_reference_df.participant_id,
        "inner",
    )
    .join(
        organization_reference_df,
        eligible_result_df.organization_id == organization_reference_df.organization_id,
        "inner",
    )
    .join(
        event_reference_df,
        eligible_result_df.event_id == event_reference_df.event_id,
        "inner",
    )
    .select(
        event_reference_df.season_year,
        organization_reference_df.organization_name,
        eligible_result_df.participant_id,
        participant_reference_df.participant_name,
        eligible_result_df.event_id,
        eligible_result_df.finish_position,
        eligible_result_df.awarded_points,
        eligible_result_df.calculated_points,
    )
)

# 8. Read existing target when present, otherwise use an empty target-shaped DataFrame
prior_event_result_df = (
    spark.table(target_table)
    if spark.catalog.tableExists(target_table)
    else spark.createDataFrame([], target_schema)
)

# 9. Reconcile incoming rows with the current target
reconciled_event_result_df = (
    prior_event_result_df.alias("tgt")
    .join(
        incoming_event_result_df.alias("upd"),
        (col("tgt.participant_id") == col("upd.participant_id")) &
        (col("tgt.event_id") == col("upd.event_id")),
        "full_outer",
    )
    .select(
        coalesce(col("tgt.season_year"), col("upd.season_year")).alias("season_year"),
        coalesce(col("tgt.organization_name"), col("upd.organization_name")).alias("organization_name"),
        coalesce(col("tgt.participant_id"), col("upd.participant_id")).alias("participant_id"),
        coalesce(col("tgt.participant_name"), col("upd.participant_name")).alias("participant_name"),
        coalesce(col("tgt.event_id"), col("upd.event_id")).alias("event_id"),
        coalesce(col("upd.finish_position"), col("tgt.finish_position")).alias("finish_position"),
        coalesce(col("upd.awarded_points"), col("tgt.awarded_points")).alias("awarded_points"),
        coalesce(col("upd.calculated_points"), col("tgt.calculated_points")).alias("calculated_points"),
        when(
            col("tgt.participant_id").isNull(),
            current_timestamp(),
        ).otherwise(col("tgt.created_at_utc")).alias("created_at_utc"),
        when(
            col("tgt.participant_id").isNotNull() & col("upd.participant_id").isNotNull(),
            current_timestamp(),
        ).otherwise(col("tgt.updated_at_utc")).alias("updated_at_utc"),
    )
)

# 10. Load reconciled target
reconciled_event_result_df.write.mode("overwrite").format("parquet").saveAsTable(target_table)

# 11. Release resources
spark.stop()
