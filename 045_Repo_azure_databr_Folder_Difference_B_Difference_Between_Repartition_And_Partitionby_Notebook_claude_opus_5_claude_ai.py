"""
Business purpose: publish the exhibitor list in three layouts, so consumers can
compare an even split across files, a split that keeps each country's exhibitors
together in the same file, and a directory tree with one folder per country.
Source: data/landing/tradefair/exhibitor_list.csv, the exhibitor register with a
header row.
Targets: warehouse/curated/tradefair/exhibitor_even_split written as four files of
roughly equal size, exhibitor_by_country_shuffle written as two files with each
country landing whole in one of them, and exhibitor_by_country_folders written as
one folder per country; all three are CSV and replaced on every run.
Output grain: one row per exhibitor, the same in all three layouts.
Business rules: the records are published unchanged, so the three targets differ
only in how the rows are distributed across files and folders.
Generation profile: portable flat batch PySpark layout comparison.
Runtime arguments: none.
Assumptions: the schema is declared instead of inferred so the column types do not
depend on the sampled rows; the original demonstrated its writes on eight records
typed into the notebook and read a real file it then never used, so the file it
read is the source here; it wrote all three layouts to one placeholder location,
which would have overwritten the previous one each time, so each layout is given
its own target; and the directory listings were there to show the result.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("tradefair_exhibitor_layout_publish").getOrCreate()

# 2. Read the exhibitor register with an explicit schema contract
exhibitor_schema = StructType(fields=[
    StructField("exhibitor_id", IntegerType(), True),
    StructField("exhibitor_name", StringType(), True),
    StructField("country_name", StringType(), True),
])

exhibitor_df = spark.read \
    .option("header", True) \
    .schema(exhibitor_schema) \
    .csv("data/landing/tradefair/exhibitor_list.csv")

# 3. Spread the register evenly across four files
exhibitor_even_df = exhibitor_df.repartition(4)

exhibitor_even_df.write \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/tradefair/exhibitor_even_split")

# 4. Send every country whole into one of two files
exhibitor_country_shuffled_df = exhibitor_df.repartition(2, "country_name")

exhibitor_country_shuffled_df.write \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/tradefair/exhibitor_by_country_shuffle")

# 5. Give every country its own folder
exhibitor_df.write \
    .partitionBy("country_name") \
    .format("csv") \
    .mode("overwrite") \
    .save("warehouse/curated/tradefair/exhibitor_by_country_folders")

# 6. Release resources
spark.stop()
