"""
Business purpose: publish one denormalized row per rental agreement line, so hire
revenue reporting reads the line together with the branch, the agreement date and
the delivery mode without joining the header itself.
Sources: the catalogued tables rental_staging.rental_agreement, one row per
agreement, and rental_staging.rental_agreement_line, one row per line of an
agreement.
Target: warehouse/curated/rental/rental_agreement_fact, a Parquet dataset written
as a single file and replaced on every run.
Output grain: one row per agreement line.
Business rules: a line is published only when its agreement is present, every
column of the line is carried through unchanged, and only the branch, the
agreement date and the delivery mode are taken from the header.
Generation profile: portable flat batch PySpark fact build.
Runtime arguments: none.
Assumptions: the catalogued tables already exist; the bucket name that the
original took as an argument only chose where the output sat, so the target is
written to a fixed path here; the row count and schema print were progress
logging; and each temporary view is named after the table it actually holds,
because the original registered each frame under the other table's name and then
swapped the aliases back in its query, which cancelled out but left every view
pointing at the wrong table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark
spark = SparkSession.builder.appName("rental_agreement_fact_build").getOrCreate()

# 2. Read the agreement lines
agreement_line_df = spark.table("rental_staging.rental_agreement_line").select(
    col("agreement_id"),
    col("line_id"),
    col("line_number"),
    col("asset_id"),
    col("quantity"),
    col("daily_rate"),
    col("discount"),
    col("tax"))

# 3. Read the agreement headers
agreement_header_df = spark.table("rental_staging.rental_agreement").select(
    col("agreement_id"),
    col("branch_id"),
    col("agreement_date"),
    col("delivery_mode"))

# 4. Expose the lines to SQL
agreement_line_df.createOrReplaceTempView("vw_rental_agreement_line")

# 5. Expose the headers to SQL
agreement_header_df.createOrReplaceTempView("vw_rental_agreement_header")

# 6. Carry the header context onto every line
rental_agreement_fact_df = spark.sql("""
    SELECT line.*,
           header.branch_id,
           header.agreement_date,
           header.delivery_mode
      FROM vw_rental_agreement_header header, vw_rental_agreement_line line
     WHERE line.agreement_id = header.agreement_id
""")

# 7. Gather the result into a single file
rental_agreement_fact_single_df = rental_agreement_fact_df.coalesce(1)

# 8. Replace the published fact
rental_agreement_fact_single_df.write \
    .mode("overwrite") \
    .parquet("warehouse/curated/rental/rental_agreement_fact")

# 9. Release resources
spark.stop()
