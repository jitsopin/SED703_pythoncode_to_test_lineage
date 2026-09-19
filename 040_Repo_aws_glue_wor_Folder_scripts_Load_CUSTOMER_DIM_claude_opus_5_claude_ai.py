"""
Business purpose: publish one denormalized row per grower farm, so analytics reads
the grower profile and the farm details together instead of joining them itself.
Sources: the catalogued tables agriculture_staging.grower and
agriculture_staging.grower_farm.
Target: warehouse/curated/agriculture/grower_dim, a Parquet dataset replaced on
every run.
Output grain: one row per farm of a grower.
Business rules: a farm is published only when its grower is present, so the join
is an inner join, and the grower identifier that the join consumed is not carried
into the published dimension.
Generation profile: portable flat batch PySpark dimension build.
Runtime arguments: none.
Assumptions: the catalogued tables already exist; the bucket name that the
original took as an argument only chose where the output sat, so the target is
written to a fixed path here; the write is an overwrite so a rerun is repeatable;
and the grower identifier is dropped after the join because the original dropped
its join key the same way, which leaves the dimension without a reference back to
the grower.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark
spark = SparkSession.builder.appName("agriculture_grower_dim_build").getOrCreate()

# 2. Read the grower profiles
grower_df = spark.table("agriculture_staging.grower").select(
    col("grower_id"),
    col("grower_name"),
    col("contact_email"),
    col("grower_segment"),
    col("registration_date"))

# 3. Read the farms of every grower
grower_farm_df = spark.table("agriculture_staging.grower_farm").select(
    col("grower_id"),
    col("farm_id"),
    col("farm_name"),
    col("address_line"),
    col("city_name"),
    col("region_code"),
    col("planted_hectares"))

# 4. Denormalize the farm with its grower and drop the consumed key
grower_dim_df = grower_df \
    .join(grower_farm_df, ["grower_id"], "inner") \
    .drop("grower_id")

# 5. Replace the published dimension
grower_dim_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/agriculture/grower_dim")

# 6. Release resources
spark.stop()
