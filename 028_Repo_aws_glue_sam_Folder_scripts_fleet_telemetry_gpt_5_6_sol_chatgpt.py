"""
Business purpose: Build a portable fleet-telemetry snapshot from four representative telemetry events.
Source: Inline telemetry event literals embedded in this standalone script.
Target: Partitioned Parquet dataset at data/curated/fleet_operations/telemetry_events.
Output grain: One row per telemetry event_id.
Important rules: Preserve the four event payloads, apply defaults for missing asset type, speed, and region, retain spatial coordinates as portable WKT text, retain sensor payloads as JSON text, derive event_date, and partition output by event_date and asset_type.
Generation profile: DATAFRAME_CHAIN + PORTABLE_TELEMETRY_FLOW
Runtime arguments: None.
Assumptions: Portable Spark does not guarantee the source engine's specialized spatial, semi-structured, or nanosecond timestamp types, so those business values are preserved as WKT, JSON, and timestamp text.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, substring
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_fleet_telemetry_pipeline").getOrCreate()

target_path = "data/curated/fleet_operations/telemetry_events"

telemetry_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("asset_id", StringType(), False),
    StructField("asset_type", StringType(), True),
    StructField("event_time_text", StringType(), False),
    StructField("location_wkt", StringType(), False),
    StructField("service_area_wkt", StringType(), False),
    StructField("sensor_payload_json", StringType(), False),
    StructField("speed_kmh", DoubleType(), True),
    StructField("region", StringType(), True),
])

# 2. Create raw telemetry events
telemetry_raw_df = spark.createDataFrame(
    [
        ("TELEM-101", "VAN-842", "VAN", "2026-07-28 09:15:30.123456789",
         "POINT(-0.1278 51.5074)", "POINT(-0.1278 51.5074)",
         '{"fuel_pct":0.72,"cargo_kg":450,"door_open":false,"engine":{"rpm":2100,"temp_c":88.5},"route":{"stops_remaining":4,"eta_minutes":35}}',
         35.2, "EMEA"),
        ("TELEM-102", "ROBOT-517", "ROBOT", "2026-07-28 09:15:30.123456790",
         "POINT(4 1)", "POINT(4 1)",
         '{"battery_pct":0.62,"obstacle_distance_m":2.8,"payload_g":1200,"navigation_mode":"autonomous","cameras":{"front":"active","rear":"recording"},"path_clear":true}',
         48.0, "EMEA"),
        ("TELEM-103", "BIKE-303", "BIKE", "2026-07-28 09:15:30.999999999",
         "POINT(3 1)", "POINT(3 1)",
         '{"cadence_rpm":72,"heart_rate_bpm":145,"assist_level":"high","battery_pct":0.55,"route_type":"cycle_lane","deliveries":{"completed":6,"remaining":4,"bag_pct_full":55}}',
         18.5, "EMEA"),
        ("TELEM-104", "ASSET-999", None, "2026-07-28 10:00:00.000000000",
         "POINT(1 1)", "POINT(1 1)", '{"status":"initializing"}', None, None),
    ],
    telemetry_schema,
)

# 3. Apply portable defaults
telemetry_defaulted_df = telemetry_raw_df.select(
    col("event_id"),
    col("asset_id"),
    coalesce(col("asset_type"), lit("UNKNOWN")).alias("asset_type"),
    col("event_time_text"),
    col("location_wkt"),
    col("service_area_wkt"),
    col("sensor_payload_json"),
    coalesce(col("speed_kmh"), lit(0.0)).alias("speed_kmh"),
    coalesce(col("region"), lit("EMEA")).alias("region"),
)

# 4. Derive output partition key
published_telemetry_df = telemetry_defaulted_df.select(
    col("event_id"),
    col("asset_id"),
    col("asset_type"),
    col("event_time_text"),
    col("location_wkt"),
    col("service_area_wkt"),
    col("sensor_payload_json"),
    col("speed_kmh"),
    col("region"),
    substring(col("event_time_text"), 1, 10).alias("event_date"),
)

# 5. Load
published_telemetry_df.write.mode("overwrite").partitionBy(
    "event_date",
    "asset_type",
).parquet(target_path)

# 6. Release resources
spark.stop()
