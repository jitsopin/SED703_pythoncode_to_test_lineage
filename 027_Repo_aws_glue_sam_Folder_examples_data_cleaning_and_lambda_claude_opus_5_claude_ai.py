"""
Business purpose: publish average repair charges per repair code and workshop so
claims review reads clean numeric amounts instead of the currency text of the
source feed.
Source: the catalogued table claims.workshop_charge_summary, whose column names
carry spaces, whose workshop identifier arrives as text and whose amounts arrive
as strings prefixed with a currency symbol.
Target: warehouse/curated/claims/workshop_charges, a Parquet dataset rebuilt on
every run, with the workshop attributes and the amounts grouped into nested
objects.
Output grain: one row per source record, that is one repair code and workshop.
Business rules: the workshop identifier must be a whole number, records whose
identifier cannot be read as one are dropped, the leading currency symbol is
stripped from the three averages before they are stored as numbers, and the
workshop and charge attributes are published as nested objects.
Generation profile: portable flat batch PySpark cleaning and nesting.
Runtime arguments: none.
Assumptions: the catalogued table already exists; the currency symbol is the only
prefix on the amounts, so it is removed with an anchored replacement instead of
the character-chopping lambda of the original, which would fail on a missing
value.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, struct

# 1. Initialize Spark
spark = SparkSession.builder.appName("claims_workshop_charges_cleaning").getOrCreate()

# 2. Read the catalogued feed, naming every column it supplies
workshop_charge_landing_df = spark.table("claims.workshop_charge_summary").select(
    col("`repair code`"),
    col("`workshop id`"),
    col("`workshop name`"),
    col("`workshop city`"),
    col("`workshop region`"),
    col("`workshop postcode`"),
    col("`service area description`"),
    col("`average quoted charge`"),
    col("`average total paid`"),
    col("`average insurer paid`"))

# 3. Standardize the names and read the identifier as a whole number
workshop_charge_typed_df = workshop_charge_landing_df.select(
    col("`repair code`").alias("repair_code"),
    col("`workshop id`").cast("long").alias("workshop_id"),
    col("`workshop name`").alias("workshop_name"),
    col("`workshop city`").alias("workshop_city"),
    col("`workshop region`").alias("workshop_region"),
    col("`workshop postcode`").cast("long").alias("workshop_postcode"),
    col("`service area description`").alias("service_area"),
    col("`average quoted charge`").alias("quoted_charge_text"),
    col("`average total paid`").alias("total_paid_text"),
    col("`average insurer paid`").alias("insurer_paid_text"))

# 4. Apply business rules: the identifier must have survived the conversion
workshop_charge_eligible_df = workshop_charge_typed_df \
    .filter(col("workshop_id").isNotNull())

# 5. Strip the leading currency symbol from the three averages
workshop_charge_cleaned_df = workshop_charge_eligible_df \
    .withColumn("quoted_charge_text", regexp_replace(col("quoted_charge_text"), "^\\$", "")) \
    .withColumn("total_paid_text", regexp_replace(col("total_paid_text"), "^\\$", "")) \
    .withColumn("insurer_paid_text", regexp_replace(col("insurer_paid_text"), "^\\$", ""))

# 6. Publish the workshop and the amounts as nested objects
workshop_charge_nested_df = workshop_charge_cleaned_df.select(
    col("repair_code"),
    struct(col("workshop_id").alias("id"),
           col("workshop_name").alias("name"),
           col("workshop_city").alias("city"),
           col("workshop_region").alias("region"),
           col("workshop_postcode").alias("postcode")).alias("workshop"),
    col("service_area"),
    struct(col("quoted_charge_text").cast("double").alias("quoted"),
           col("total_paid_text").cast("double").alias("total_paid"),
           col("insurer_paid_text").cast("double").alias("insurer_paid")).alias("charges"))

# 7. Load the rebuilt dataset into the curated layer
workshop_charge_nested_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/claims/workshop_charges")

# 8. Release resources
spark.stop()
