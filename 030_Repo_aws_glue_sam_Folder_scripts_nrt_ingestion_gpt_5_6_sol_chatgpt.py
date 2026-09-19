"""
Business purpose: Build a portable near-real-time trade-risk ingestion stream from Kafka into partitioned Parquet.
Source: Kafka topic trade-risk-vectors.
Target: Partitioned Parquet dataset at data/curated/risk_stream/trade_risk_vectors.
Output grain: One row per Kafka trade event.
Important rules: Parse trade JSON payloads, preserve pricing payload as JSON text, apply defaults for missing asset_class, var_contribution, risk_weight, and region, stamp trade_date from the processing date, append in micro-batches, and checkpoint progress.
Generation profile: DATAFRAME_CHAIN + PORTABLE_NRT_STREAMING_FLOW
Runtime arguments: First optional argument supplies Kafka bootstrap servers; second optional argument supplies checkpoint root. Defaults are "localhost:9092" and "data/checkpoints".
Assumptions: Standard Kafka connectivity is available to Spark; AWS-specific IAM authentication and Iceberg-specific table features are intentionally omitted for portability.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_date, get_json_object, lit

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_trade_risk_streaming_pipeline").getOrCreate()

bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
checkpoint_root = sys.argv[2] if len(sys.argv) > 2 else "data/checkpoints"

source_topic = "trade-risk-vectors"
target_path = "data/curated/risk_stream/trade_risk_vectors"
checkpoint_path = f"{checkpoint_root}/trade_risk_vectors"

# 2. Read Kafka stream
kafka_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", source_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# 3. Decode Kafka value
trade_json_df = kafka_raw_df.select(
    col("value").cast("string").alias("json_str")
)

# 4. Parse trade payload
trade_parsed_df = trade_json_df.select(
    get_json_object(col("json_str"), "$.trade_id").alias("trade_id"),
    get_json_object(col("json_str"), "$.book_id").alias("book_id"),
    get_json_object(col("json_str"), "$.desk").alias("desk"),
    get_json_object(col("json_str"), "$.asset_class").alias("asset_class"),
    get_json_object(col("json_str"), "$.execution_time").alias("execution_time"),
    get_json_object(col("json_str"), "$.pricing_vector").alias("pricing_payload_json"),
    get_json_object(col("json_str"), "$.var_contribution").cast("double").alias("var_contribution"),
    get_json_object(col("json_str"), "$.risk_weight").cast("double").alias("risk_weight"),
    get_json_object(col("json_str"), "$.region").alias("region"),
)

# 5. Apply defaults and processing date
published_trade_stream_df = trade_parsed_df.select(
    col("trade_id"),
    col("book_id"),
    col("desk"),
    coalesce(col("asset_class"), lit("UNKNOWN")).alias("asset_class"),
    col("execution_time"),
    col("pricing_payload_json"),
    coalesce(col("var_contribution"), lit(0.0)).alias("var_contribution"),
    coalesce(col("risk_weight"), lit(1.0)).alias("risk_weight"),
    current_date().alias("trade_date"),
    coalesce(col("region"), lit("EMEA")).alias("region"),
)

# 6. Start append stream
stream_query = (
    published_trade_stream_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", target_path)
    .option("checkpointLocation", checkpoint_path)
    .partitionBy("trade_date", "asset_class")
    .trigger(processingTime="10 seconds")
    .start()
)

# 7. Keep streaming until externally stopped
stream_query.awaitTermination()

# 8. Release resources after termination
spark.stop()
