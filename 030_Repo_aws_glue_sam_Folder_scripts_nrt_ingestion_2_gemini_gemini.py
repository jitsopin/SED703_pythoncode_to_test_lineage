"""Business purpose: Near Real-Time (NRT) ingestion of energy grid smart meter readings.
Source: kafka://streaming-cluster:9092/smart-meter-telemetry
Target: warehouse/energy/smart_meters (Partitioned Parquet)
Output grain: One record per smart meter reading event.
Business rules: Consume streaming JSON payloads, extract schema fields natively, apply structural defaults, append partition dates, and stream to storage in 10-second micro-batches.
Generation profile: Portable PySpark Structured Streaming; AWS Glue, IAM Auth, and raw Iceberg SQL statements replaced with native DataFrame APIs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_date, get_json_object, lit

# 1. Initialize SparkSession and configuration variables
spark = SparkSession.builder.appName("EnergyGridNRT_Ingestion").getOrCreate()
kafka_bootstrap = "streaming-cluster:9092"
checkpoint_dir = "warehouse/checkpoints/smart_meters_nrt"

# 2. Extract raw stream from Kafka
meter_raw_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "smart-meter-telemetry") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Parse binary Kafka value to JSON string
meter_parsed_df = meter_raw_stream_df.select(col("value").cast("string").alias("json_str"))

# 4. Extract columns, apply defaults, and enrich with partition keys
meter_enriched_df = meter_parsed_df.select(
    get_json_object(col("json_str"), "$.meter_id").alias("meter_id"),
    get_json_object(col("json_str"), "$.grid_id").alias("grid_id"),
    get_json_object(col("json_str"), "$.substation").alias("substation"),
    coalesce(get_json_object(col("json_str"), "$.energy_source"), lit("UNKNOWN")).alias("energy_source"),
    get_json_object(col("json_str"), "$.reading_timestamp").alias("reading_timestamp"),
    get_json_object(col("json_str"), "$.power_metrics").alias("power_metrics_payload"),
    coalesce(get_json_object(col("json_str"), "$.voltage_drop").cast("double"), lit(0.0)).alias("voltage_drop"),
    coalesce(get_json_object(col("json_str"), "$.load_factor").cast("double"), lit(1.0)).alias("load_factor"),
    current_date().alias("reading_date"),
    coalesce(get_json_object(col("json_str"), "$.region"), lit("EMEA")).alias("region")
)

# 5. Sink stream to storage in micro-batches
streaming_query = meter_enriched_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("reading_date", "energy_source") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start("warehouse/energy/smart_meters")

# Note: streaming_query.awaitTermination() omitted for flat script continuity representation