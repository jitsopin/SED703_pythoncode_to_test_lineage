"""
Business purpose: publish one measured row per banquet order line so revenue and
margin reporting reads the line value and the profit already worked out.
Sources: the catalogued tables banquet_staging.incr_banquet_order, one row per
booking, and banquet_staging.incr_banquet_order_line, one row per line of a
booking; both hold the increment loaded since the previous run.
Target: the table analytics_public.banquet_order_fact in the reporting warehouse,
appended over JDBC.
Output grain: one row per booking and order line.
Business rules: a line is published only when its booking is present, the extended
price is the quantity times the unit price rounded to two decimals, the profit is
the quantity times the margin between unit price and supply cost rounded the same
way, the date key is the event date written as yyyyMMdd, the venue identifier is
published as a whole number, and every column is published under its lower case
name.
Generation profile: portable flat batch PySpark incremental fact load.
Runtime arguments: argv[1] is the JDBC url of the reporting warehouse, argv[2] the
user and argv[3] the password; they fall back to a local database, "warehouse" and
an empty password.
Assumptions: the staging tables already hold the increment for this run; the row
counts and schema prints of the original were progress logging, not business
rules; and the warehouse is written over JDBC because the managed warehouse sink
of the original is not portable.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark and read the runtime arguments
spark = SparkSession.builder.appName("banquet_order_fact_incremental_load").getOrCreate()
jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/warehouse"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "warehouse"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else ""

# 2. Read the booking headers of this increment
booking_header_df = spark.table("banquet_staging.incr_banquet_order").select(
    col("BOOKING_ID"),
    col("EVENT_DATE"),
    col("SERVICE_MODE"),
    col("VENUE_ID"))

# 3. Read the booking lines of this increment
booking_line_df = spark.table("banquet_staging.incr_banquet_order_line").select(
    col("BOOKING_ID"),
    col("LINE_ID"),
    col("LINE_NUMBER"),
    col("ITEM_ID"),
    col("QUANTITY"),
    col("UNIT_PRICE"),
    col("DISCOUNT"),
    col("TAX"),
    col("SUPPLY_COST"))

# 4. Attach every line to the booking it belongs to
booking_joined_df = booking_header_df.join(booking_line_df, ["BOOKING_ID"], "inner")

# 5. Expose the joined rows to SQL
booking_joined_df.createOrReplaceTempView("vw_booking_joined")

# 6. Work out the line value, the profit and the date key
booking_enriched_df = spark.sql("""
    SELECT booking.*,
           bround(booking.QUANTITY * booking.UNIT_PRICE, 2) AS EXTENDED_PRICE,
           bround(booking.QUANTITY * (booking.UNIT_PRICE - booking.SUPPLY_COST), 2) AS PROFIT,
           date_format(booking.EVENT_DATE, 'yyyyMMdd') AS DATE_KEY
      FROM vw_booking_joined booking
""")

# 7. Publish every column under its lower case name
banquet_order_fact_df = booking_enriched_df.select(
    col("BOOKING_ID").alias("booking_id"),
    col("EVENT_DATE").alias("event_date"),
    col("SERVICE_MODE").alias("service_mode"),
    col("VENUE_ID").cast("int").alias("venue_id"),
    col("LINE_ID").alias("line_id"),
    col("LINE_NUMBER").alias("line_number"),
    col("ITEM_ID").alias("item_id"),
    col("QUANTITY").alias("quantity"),
    col("UNIT_PRICE").alias("unit_price"),
    col("DISCOUNT").alias("discount"),
    col("TAX").alias("tax"),
    col("SUPPLY_COST").alias("supply_cost"),
    col("EXTENDED_PRICE").alias("extended_price"),
    col("PROFIT").alias("profit"),
    col("DATE_KEY").alias("date_key"))

# 8. Append the increment to the reporting warehouse
banquet_order_fact_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "analytics_public.banquet_order_fact") \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .mode("append") \
    .save()

# 9. Release resources
spark.stop()
