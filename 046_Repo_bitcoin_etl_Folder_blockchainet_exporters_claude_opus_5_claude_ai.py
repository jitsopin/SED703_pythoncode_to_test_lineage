"""
Business purpose: publish the consignment declarations in the two shapes
downstream consumers ask for, a flat CSV for spreadsheet users and one JSON
document per line for systems that read the file as a stream.
Source: data/curated/freight/consignment_declaration, the curated declarations,
in which the commodity codes are held as a list and the gross weight is held at
full decimal precision.
Targets: warehouse/export/freight/consignment_declaration_csv, written with a
header row, and warehouse/export/freight/consignment_declaration_jsonl, written as
one JSON document per record; both are replaced on every run.
Output grain: one row per consignment_id in both exports.
Business rules: the CSV export joins the commodity codes into one comma separated
value because a flat file has no place for a list, and writes an empty string
where the consignee is missing so every row has the same number of columns; the
JSON export rounds the gross weight to eight decimal places, which is the
precision the serializer publishes.
Generation profile: portable flat batch PySpark two format export.
Runtime arguments: none.
Assumptions: the source file is a serialization library with no datasets of its
own, so this pipeline is not extracted from it; the three rules above are the
behaviours it implements, namely joining multi valued fields for CSV, writing a
default empty value for fields that are absent, and rounding decimal values to
eight places before encoding them as JSON numbers; the datasets and the column set
are named here and none of them appear in the original.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, lit, round as spark_round

# 1. Initialize Spark
spark = SparkSession.builder.appName("freight_consignment_export").getOrCreate()

# 2. Read the curated declarations
consignment_df = spark.read.parquet("data/curated/freight/consignment_declaration").select(
    col("consignment_id"),
    col("declared_at"),
    col("commodity_codes"),
    col("gross_weight_kg"),
    col("consignee_name"),
    col("origin_country"))

# 3. Flatten the list and fill the missing consignee for the flat file
consignment_csv_df = consignment_df.select(
    col("consignment_id"),
    col("declared_at"),
    concat_ws(",", col("commodity_codes")).alias("commodity_codes"),
    col("gross_weight_kg"),
    coalesce(col("consignee_name"), lit("")).alias("consignee_name"),
    col("origin_country"))

# 4. Write the flat file with a header row
consignment_csv_df.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/export/freight/consignment_declaration_csv")

# 5. Round the gross weight to the precision the JSON export publishes
consignment_jsonl_df = consignment_df.select(
    col("consignment_id"),
    col("declared_at"),
    col("commodity_codes"),
    spark_round(col("gross_weight_kg"), 8).alias("gross_weight_kg"),
    col("consignee_name"),
    col("origin_country"))

# 6. Write one JSON document per record
consignment_jsonl_df.write \
    .format("json") \
    .mode("overwrite") \
    .save("warehouse/export/freight/consignment_declaration_jsonl")

# 7. Release resources
spark.stop()
