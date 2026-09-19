"""
Business purpose: land loan risk vectors into the curated table as they are
published, so downstream risk reporting reads a table that is never more than one
trigger behind the stream.
Source: the Kafka topic loan-risk-vectors, whose messages carry one loan risk
vector as a JSON document.
Target: the table lending_curated.loan_risk_vectors, appended continuously.
Output grain: one row per message, that is one loan risk vector.
Business rules: every field is read out of the message body by its JSON path, the
amounts are stored as numbers, the pricing payload is kept whole as JSON text for
downstream extraction, and snapshot_date records the day the message was landed.
Generation profile: portable PySpark structured streaming ingestion.
Runtime arguments: argv[1] is the Kafka bootstrap server list; it falls back to
"localhost:9092".
Assumptions: the target table already exists; the broker authentication of the
original was a managed-service login module and is left to the cluster
configuration; the payload is kept as JSON text rather than the Iceberg variant
type, which is not portable; and the run is bounded by a five minute wait instead
of a fixed sleep, matching how long the original let the stream run.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, get_json_object

# 1. Initialize Spark and read the runtime argument
spark = SparkSession.builder.appName("lending_loan_risk_vectors_nrt_ingestion").getOrCreate()
bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"

# 2. Subscribe to the topic and read every message body as text
loan_risk_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "loan-risk-vectors") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(col("value").cast("string").alias("json_str"))

# 3. Read every business field out of the message body
loan_risk_stream_df = loan_risk_raw_df.select(
    get_json_object(col("json_str"), "$.loan_id").alias("loan_id"),
    get_json_object(col("json_str"), "$.portfolio_id").alias("portfolio_id"),
    get_json_object(col("json_str"), "$.branch").alias("branch"),
    get_json_object(col("json_str"), "$.product_class").alias("product_class"),
    get_json_object(col("json_str"), "$.execution_time").alias("execution_time"),
    get_json_object(col("json_str"), "$.pricing_vector").alias("pricing_vector"),
    get_json_object(col("json_str"), "$.loss_contribution").cast("double").alias("loss_contribution"),
    get_json_object(col("json_str"), "$.risk_weight").cast("double").alias("risk_weight"),
    current_date().alias("snapshot_date"),
    get_json_object(col("json_str"), "$.region").alias("region"))

# 4. Append every trigger to the curated table
loan_risk_query = loan_risk_stream_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "warehouse/checkpoints/loan_risk_vectors") \
    .trigger(processingTime="10 seconds") \
    .toTable("lending_curated.loan_risk_vectors")

# 5. Let the stream run, then close it down
loan_risk_query.awaitTermination(300)
loan_risk_query.stop()

# 6. Release resources
spark.stop()
