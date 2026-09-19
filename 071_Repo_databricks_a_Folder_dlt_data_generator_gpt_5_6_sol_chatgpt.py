"""
Business purpose: Continuously generate portable apparel-retail source streams for sales, customers, products, and stores so downstream pipelines can exercise CDC, data quality, referential-integrity, and late-arriving-event logic.
Sources: Four Spark rate streams used only as portable record clocks.
Targets: Parquet datasets at data/landing/apparel/sales, data/landing/apparel/customers, data/landing/apparel/products, and data/landing/apparel/stores.
Output grain: Sales has one row per synthetic transaction; customers/products/stores have one row per generated dimension change event.
Important rules: Preserve the four source schemas; generate repeated dimension IDs to simulate Type-1-style updates; generate sales customer/product/store IDs only from valid configured ID ranges; allow negative sales quantity and discount values; allow missing customer email and store manager values; skew sales event_time and dimension last_update_time by up to 60 seconds; append continuously with independent checkpoints.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (PORTABLE_MULTI_STREAM_DATA_GENERATOR)
Runtime arguments: None.
Assumptions: Databricks notebook magic, Faker, Delta transaction-log state, variables.py path constants, ThreadPoolExecutor orchestration, and exact restart counters are environment-specific. This rewrite preserves the observable schemas and test-data behaviors with id-based Spark expressions plus rand() while using portable rate streams and Parquet sinks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr, lit, pmod, rand, round as spark_round, when

spark = SparkSession.builder.appName("portable_apparel_data_generator").getOrCreate()

sales_output_path = "data/landing/apparel/sales"
customers_output_path = "data/landing/apparel/customers"
products_output_path = "data/landing/apparel/products"
stores_output_path = "data/landing/apparel/stores"
sales_checkpoint_path = "data/landing/apparel/checkpoints/sales"
customers_checkpoint_path = "data/landing/apparel/checkpoints/customers"
products_checkpoint_path = "data/landing/apparel/checkpoints/products"
stores_checkpoint_path = "data/landing/apparel/checkpoints/stores"

customer_id_domain = 200
product_id_domain = 100
store_id_domain = 25

sales_clock_df = spark.readStream.format("rate").option("rowsPerSecond", 17).load()

sales_enriched_df = (
    sales_clock_df
    .withColumn("transaction_id", (col("value") + lit(1)).cast("long"))
    .withColumn("store_id", (pmod(col("value"), lit(store_id_domain)) + lit(1)).cast("int"))
    .withColumn("customer_id", (pmod(col("value") * lit(7), lit(customer_id_domain)) + lit(1)).cast("int"))
    .withColumn("product_id", (pmod(col("value") * lit(11), lit(product_id_domain)) + lit(1)).cast("int"))
    .withColumn("quantity_base", (pmod(col("value"), lit(5)) + lit(1)).cast("int"))
    .withColumn("quantity", when(pmod(col("value"), lit(20)) == lit(0), -col("quantity_base")).otherwise(col("quantity_base")))
    .withColumn("unit_price", spark_round(lit(10.0) + rand(seed=101) * lit(90.0), 2))
    .withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))
    .withColumn("discount_base", spark_round(rand(seed=102) * lit(20.0), 2))
    .withColumn("discount_applied", when(pmod(col("value"), lit(50)) == lit(0), -col("discount_base")).otherwise(col("discount_base")))
    .withColumn("tax_amount", spark_round(col("total_amount") * lit(0.08), 2))
    .withColumn("payment_method", expr("element_at(array('Cash','Credit Card','Debit Card','Mobile Pay','Gift Card'), pmod(CAST(value AS INT), 5) + 1)"))
    .withColumn("event_time", expr("current_timestamp() - INTERVAL 1 SECOND * pmod(CAST(value AS INT), 61)"))
)

sales_output_df = sales_enriched_df.select(
    col("transaction_id"), col("store_id"), col("event_time"), col("customer_id"), col("product_id"),
    col("quantity"), col("unit_price"), col("total_amount"), col("payment_method"), col("discount_applied"), col("tax_amount")
)

customers_clock_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
customers_output_df = customers_clock_df.select(
    (pmod(col("value"), lit(customer_id_domain)) + lit(1)).cast("int").alias("customer_id"),
    concat(lit("Customer "), (pmod(col("value"), lit(customer_id_domain)) + lit(1))).alias("name"),
    when(pmod(col("value"), lit(50)) == lit(0), lit(None).cast("string")).otherwise(concat(lit("customer"), (pmod(col("value"), lit(customer_id_domain)) + lit(1)), lit("@example.test"))).alias("email"),
    concat(lit("Address "), (pmod(col("value"), lit(500)) + lit(1))).alias("address"),
    expr("current_timestamp() - INTERVAL 1 DAY * (pmod(CAST(value AS INT), 365) + 1)").alias("join_date"),
    pmod(col("value") * lit(13), lit(1001)).cast("int").alias("loyalty_points"),
    concat(lit("+1 "), (lit(100000000) + pmod(col("value"), lit(899999999)))).alias("phone_number"),
    (lit(18) + pmod(col("value"), lit(53))).cast("int").alias("age"),
    expr("element_at(array('Male','Female','Other'), pmod(CAST(value AS INT), 3) + 1)").alias("gender"),
    expr("current_timestamp() - INTERVAL 1 SECOND * pmod(CAST(value AS INT), 61)").alias("last_update_time")
)

products_clock_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
products_output_df = products_clock_df.select(
    (pmod(col("value"), lit(product_id_domain)) + lit(1)).cast("int").alias("product_id"),
    concat(lit("Product "), (pmod(col("value"), lit(product_id_domain)) + lit(1))).alias("name"),
    expr("element_at(array('Casual Wear','Formal Wear','Sportswear','Accessories','Footwear'), pmod(CAST(value AS INT), 5) + 1)").alias("category"),
    expr("element_at(array('Urban Threads','Peak Performance','Vivid Apparel','Heritage Denim','Metro Style'), pmod(CAST(value AS INT), 5) + 1)").alias("brand"),
    spark_round(lit(10.0) + rand(seed=201) * lit(140.0), 2).alias("price"),
    pmod(col("value") * lit(17), lit(201)).cast("int").alias("stock_quantity"),
    expr("element_at(array('XS','S','M','L','XL'), pmod(CAST(value AS INT), 5) + 1)").alias("size"),
    expr("element_at(array('Red','Blue','Green','Black','White','Yellow','Gray','Purple','Orange','Pink'), pmod(CAST(value AS INT), 10) + 1)").alias("color"),
    concat(lit("Synthetic apparel item "), (pmod(col("value"), lit(product_id_domain)) + lit(1))).alias("description"),
    expr("current_timestamp() - INTERVAL 1 SECOND * pmod(CAST(value AS INT), 61)").alias("last_update_time")
)

stores_clock_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
stores_output_df = stores_clock_df.select(
    (pmod(col("value"), lit(store_id_domain)) + lit(1)).cast("int").alias("store_id"),
    concat(lit("Store "), (pmod(col("value"), lit(store_id_domain)) + lit(1))).alias("name"),
    concat(lit("Store Address "), (pmod(col("value"), lit(200)) + lit(1))).alias("address"),
    when(pmod(col("value"), lit(50)) == lit(0), lit(None).cast("string")).otherwise(concat(lit("Manager "), (pmod(col("value"), lit(store_id_domain)) + lit(1)))).alias("manager"),
    expr("current_timestamp() - INTERVAL 1 DAY * (365 + pmod(CAST(value AS INT), 1461))").alias("open_date"),
    when(pmod(col("value"), lit(10)) == lit(0), lit("Under Renovation")).otherwise(lit("Open")).alias("status"),
    concat(lit("+1 "), (lit(200000000) + pmod(col("value"), lit(799999999)))).alias("phone_number"),
    expr("current_timestamp() - INTERVAL 1 SECOND * pmod(CAST(value AS INT), 61)").alias("last_update_time")
)

sales_query = sales_output_df.writeStream.format("parquet").outputMode("append").option("path", sales_output_path).option("checkpointLocation", sales_checkpoint_path).start()
customers_query = customers_output_df.writeStream.format("parquet").outputMode("append").option("path", customers_output_path).option("checkpointLocation", customers_checkpoint_path).start()
products_query = products_output_df.writeStream.format("parquet").outputMode("append").option("path", products_output_path).option("checkpointLocation", products_checkpoint_path).start()
stores_query = stores_output_df.writeStream.format("parquet").outputMode("append").option("path", stores_output_path).option("checkpointLocation", stores_checkpoint_path).start()

spark.streams.awaitAnyTermination()
sales_query.stop()
customers_query.stop()
products_query.stop()
stores_query.stop()
spark.stop()
