"""
Business purpose: Demonstrate portable resolution strategies for a mixed-type provider identifier and publish a filtered high-discharge branch.
Source: Parquet healthcare provider records at data/landing/healthcare_choice/provider_records.
Targets: Five JSON datasets under data/curated/healthcare_choice/.
Output grain: Choice-resolution branches preserve one row per source record except the projected-long branch; the filtered branch keeps rows with total_discharges > 30.
Important rules: Provide distinct cast, projected-long, split-column, and struct representations for provider_id, preserve supporting fields, and publish a high-discharge subset.
Generation profile: DATAFRAME_CHAIN + CHOICE_RESOLUTION_PORT
Runtime arguments: None.
Assumptions: provider_id_raw may contain numeric or non-numeric text; source-specific choice semantics are represented explicitly with Spark columns.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

spark = SparkSession.builder.appName("portable_provider_choice_resolution_pipeline").getOrCreate()

source_path = "data/landing/healthcare_choice/provider_records"
cast_target_path = "data/curated/healthcare_choice/provider_id_cast"
project_target_path = "data/curated/healthcare_choice/provider_id_projected_long"
columns_target_path = "data/curated/healthcare_choice/provider_id_split_columns"
struct_target_path = "data/curated/healthcare_choice/provider_id_struct"
filtered_target_path = "data/curated/healthcare_choice/high_discharge_records"

payment_schema = StructType([
    StructField("provider_id_raw", StringType(), True),
    StructField("total_discharges", LongType(), True),
    StructField("record_payload", StringType(), True),
])

payment_raw_df = spark.read.schema(payment_schema).parquet(source_path)

provider_cast_df = payment_raw_df.select(
    col("provider_id_raw").cast("long").alias("provider_id"),
    col("total_discharges"),
    col("record_payload"),
)

provider_projected_df = payment_raw_df.filter(
    col("provider_id_raw").cast("long").isNotNull()
).select(
    col("provider_id_raw").cast("long").alias("provider_id"),
    col("total_discharges"),
    col("record_payload"),
)

provider_split_df = payment_raw_df.select(
    when(col("provider_id_raw").cast("long").isNotNull(), col("provider_id_raw").cast("long")).alias("provider_id_long"),
    when(col("provider_id_raw").cast("long").isNull(), col("provider_id_raw")).alias("provider_id_string"),
    col("total_discharges"),
    col("record_payload"),
)

provider_struct_df = payment_raw_df.select(
    struct(
        col("provider_id_raw").cast("long").alias("long_value"),
        col("provider_id_raw").alias("string_value"),
    ).alias("provider_id_choice"),
    col("total_discharges"),
    col("record_payload"),
)

high_discharge_df = payment_raw_df.filter(col("total_discharges") > 30)

provider_cast_df.write.mode("overwrite").json(cast_target_path)
provider_projected_df.write.mode("overwrite").json(project_target_path)
provider_split_df.write.mode("overwrite").json(columns_target_path)
provider_struct_df.write.mode("overwrite").json(struct_target_path)
high_discharge_df.write.mode("overwrite").json(filtered_target_path)

spark.stop()
