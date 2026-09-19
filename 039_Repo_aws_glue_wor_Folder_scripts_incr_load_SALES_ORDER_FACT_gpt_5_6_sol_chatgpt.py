"""
Business purpose: Build an incremental sales-order fact dataset by joining order headers to order details, deriving pricing/profit/date measures, standardizing field names/types, and loading the result to a relational fact table.
Sources: Parquet datasets at data/landing/sales_incremental/orders and data/landing/sales_incremental/order_details.
Target: JDBC table analytics.sales_order_fact.
Output grain: One fact row per matched sales-order detail line.
Important rules: Inner join on order_id; extended_price = round(quantity * unit_price, 2); profit = round(quantity * (unit_price - supply_cost), 2); date_key = order_date formatted yyyyMMdd; cast site_id to integer; preserve the mapped fact columns.
Generation profile: DATAFRAME_CHAIN + INCREMENTAL_SALES_FACT
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: Source datasets expose the columns referenced by the original mappings and the PostgreSQL JDBC driver is available.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import bround, col, date_format
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("portable_incremental_sales_order_fact").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/analytics"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "analytics_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

orders_path = "data/landing/sales_incremental/orders"
order_details_path = "data/landing/sales_incremental/order_details"
target_table = "analytics.sales_order_fact"

orders_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("ORDER_DATE", DateType(), True),
    StructField("SHIP_MODE", StringType(), True),
    StructField("SITE_ID", DoubleType(), True),
])

order_details_schema = StructType([
    StructField("ORDER_ID", IntegerType(), False),
    StructField("LINE_ID", IntegerType(), True),
    StructField("LINE_NUMBER", IntegerType(), True),
    StructField("PRODUCT_ID", IntegerType(), True),
    StructField("QUANTITY", IntegerType(), True),
    StructField("UNIT_PRICE", DecimalType(10, 2), True),
    StructField("SUPPLY_COST", DecimalType(10, 2), True),
    StructField("DISCOUNT", DecimalType(10, 2), True),
    StructField("TAX", DecimalType(10, 2), True),
])

orders_df = (
    spark.read
    .schema(orders_schema)
    .parquet(orders_path)
)

order_details_df = (
    spark.read
    .schema(order_details_schema)
    .parquet(order_details_path)
)

sales_joined_df = orders_df.join(
    order_details_df,
    orders_df.ORDER_ID == order_details_df.ORDER_ID,
    "inner",
).drop(order_details_df.ORDER_ID)

sales_enriched_df = sales_joined_df.select(
    col("DISCOUNT"),
    col("UNIT_PRICE"),
    col("TAX"),
    col("SUPPLY_COST"),
    col("PRODUCT_ID"),
    col("QUANTITY"),
    col("LINE_ID"),
    col("LINE_NUMBER"),
    col("ORDER_DATE"),
    col("SHIP_MODE"),
    col("SITE_ID"),
    col("ORDER_ID"),
    bround(col("QUANTITY") * col("UNIT_PRICE"), 2).alias("EXTENDED_PRICE"),
    bround(col("QUANTITY") * (col("UNIT_PRICE") - col("SUPPLY_COST")), 2).alias("PROFIT"),
    date_format(col("ORDER_DATE"), "yyyyMMdd").alias("DATE_KEY"),
)

sales_fact_df = sales_enriched_df.select(
    col("DISCOUNT").cast(DecimalType(10, 2)).alias("discount"),
    col("UNIT_PRICE").cast(DecimalType(10, 2)).alias("unit_price"),
    col("TAX").cast(DecimalType(10, 2)).alias("tax"),
    col("SUPPLY_COST").cast(DecimalType(10, 2)).alias("supply_cost"),
    col("PRODUCT_ID").cast(IntegerType()).alias("product_id"),
    col("QUANTITY").cast(IntegerType()).alias("quantity"),
    col("LINE_ID").cast(IntegerType()).alias("line_id"),
    col("LINE_NUMBER").cast(IntegerType()).alias("line_number"),
    col("ORDER_DATE").cast(DateType()).alias("order_date"),
    col("SHIP_MODE").cast(StringType()).alias("ship_mode"),
    col("SITE_ID").cast(IntegerType()).alias("site_id"),
    col("PROFIT").cast(DecimalType(10, 2)).alias("profit"),
    col("EXTENDED_PRICE").cast(DecimalType(10, 2)).alias("extended_price"),
    col("DATE_KEY").cast(StringType()).alias("date_key"),
    col("ORDER_ID").cast(IntegerType()).alias("order_id"),
)

(
    sales_fact_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

spark.stop()
