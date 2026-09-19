"""
Business purpose: Demonstrate three portable Spark output-partitioning strategies using a small people-by-country dataset.
Source: An in-script literal DataFrame containing id, name, and country.
Targets: Three CSV datasets under data/curated/partitioning_demo/ for fixed-count repartitioning, country-key repartitioning, and partitionBy(country).
Output grain: One row per input person in each output branch.
Important rules: Preserve repartition(4), repartition(2, "country"), and write.partitionBy("country") behaviors; overwrite each target.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (PARTITIONING_DEMO)
Runtime arguments: None.
Assumptions: The notebook's external CSV read and filesystem listing are tutorial diagnostics and do not feed the demonstrated write branches, so they are excluded.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("portable_partitioning_demo").getOrCreate()

fixed_partition_target = "data/curated/partitioning_demo/repartition_4"
country_repartition_target = "data/curated/partitioning_demo/repartition_country"
country_partitionby_target = "data/curated/partitioning_demo/partition_by_country"

people_source_df = spark.createDataFrame(
    [
        (1, "Ricki", "Russia"),
        (2, "Madelaine", "Russia"),
        (3, "Neil", "Brazil"),
        (4, "Lock", "Brazil"),
        (5, "Prissie", "India"),
        (6, "Myrilla", "India"),
        (7, "Correna", "USA"),
        (8, "Ricki", "USA"),
    ],
    schema=["id", "name", "country"],
)

people_repartition4_df = people_source_df.repartition(4)

people_repartition4_df.write.mode("overwrite").option("header", True).csv(
    fixed_partition_target
)

people_country_repartition_df = people_source_df.repartition(2, "country")

people_country_repartition_df.write.mode("overwrite").option("header", True).csv(
    country_repartition_target
)

people_partitionby_df = people_source_df.select(
    "id",
    "name",
    "country",
)

people_partitionby_df.write.mode("overwrite").option("header", True).partitionBy(
    "country"
).csv(country_partitionby_target)

spark.stop()
