"""
Business purpose: publish the grant award register in four layouts, so consumers
that read by year, by year and country, or by country alone each get a directory
tree that matches how they filter.
Source: data/landing/research/grant_awards.csv, the award register with a header
row.
Targets: warehouse/curated/research/grant_awards_by_year partitioned by award
year, grant_awards_by_year_country partitioned by year then country,
grant_awards_by_country_single partitioned by country and written as one file per
country, and grant_awards_by_country_capped partitioned by country with at most
two records per file; all four are CSV and replaced on every run.
Output grain: one row per award record, the same in all four layouts.
Business rules: the records are published unchanged, so the four targets differ
only in how the rows are laid out on disk.
Generation profile: portable flat batch PySpark partitioned publish.
Runtime arguments: none.
Assumptions: the schema is declared instead of inferred so the column types do not
depend on the sampled rows; the original demonstrated its writes on nine records
typed into the notebook and read a real file it then never used, so the file it
read is the source here; and the directory listings and the read back of a single
partition were there to show the result, not to produce anything.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("research_grant_award_partitioned_publish").getOrCreate()

# 2. Read the award register with an explicit schema contract
grant_award_schema = StructType(fields=[
    StructField("recipient_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("award_year", IntegerType(), True),
    StructField("programme", StringType(), True),
    StructField("tier_one_awards", IntegerType(), True),
    StructField("tier_two_awards", IntegerType(), True),
    StructField("tier_three_awards", IntegerType(), True),
    StructField("total_awards", IntegerType(), True),
])

grant_award_df = spark.read \
    .option("header", True) \
    .schema(grant_award_schema) \
    .csv("data/landing/research/grant_awards.csv")

# 3. Publish the register split by award year
grant_award_df.write \
    .option("header", True) \
    .partitionBy("award_year") \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/research/grant_awards_by_year")

# 4. Publish the register split by award year and then country
grant_award_df.write \
    .option("header", True) \
    .partitionBy("award_year", "country_name") \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/research/grant_awards_by_year_country")

# 5. Gather the register into one file per country
grant_award_single_df = grant_award_df.repartition(1)

grant_award_single_df.write \
    .option("header", True) \
    .partitionBy("country_name") \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/research/grant_awards_by_country_single")

# 6. Publish the register by country, capped at two records per file
grant_award_df.write \
    .option("header", True) \
    .option("maxRecordsPerFile", 2) \
    .partitionBy("country_name") \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/research/grant_awards_by_country_capped")

# 7. Release resources
spark.stop()
