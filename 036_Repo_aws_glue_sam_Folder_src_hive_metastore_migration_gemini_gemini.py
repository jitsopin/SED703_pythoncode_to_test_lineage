"""Business purpose: Archive and flatten industrial machinery telemetry.
Source: data/landing/industrial/telemetry_dumps.json
Target: warehouse/industrial/machinery_telemetry (Partitioned Parquet)
Output grain: One record per individual sensor reading event.
Business rules: Parse deeply nested JSON structures, extract target hardware IDs, unnest sensor metric payloads, and partition by plant region.
Generation profile: Flat portable PySpark script; proprietary AWS Glue Hive/Catalog migration logic entirely replaced.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("IndustrialTelemetryArchival").getOrCreate()

# 2. Define complex nested schema mirroring hardware metadata specifications
telemetry_schema = StructType([
    StructField("plant_region", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("machine", ArrayType(
        StructType([
            StructField("machine_id", StringType(), True),
            StructField("model_number", StringType(), True),
            StructField("operating_mode", StringType(), True)
        ])
    ), True),
    StructField("measurements", ArrayType(
        StructType([
            StructField("sensor_guid", StringType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("vibration_hz", DoubleType(), True),
            StructField("pressure_psi", IntegerType(), True)
        ])
    ), True)
])

# Extract raw telemetry using defined complex schema
telemetry_raw_df = spark.read.schema(telemetry_schema).json("data/landing/industrial/telemetry_dumps.json")

# 3. Flatten complex array structs into standardized analytical columns
telemetry_flat_df = telemetry_raw_df.select(
    col("plant_region"),
    col("event_timestamp"),
    expr("element_at(machine, 1).machine_id").alias("machine_id"),
    expr("element_at(machine, 1).operating_mode").alias("operating_mode"),
    expr("element_at(measurements, 1).sensor_guid").alias("sensor_guid"),
    expr("element_at(measurements, 1).temperature_c").alias("temperature_c"),
    expr("element_at(measurements, 1).vibration_hz").alias("vibration_hz"),
    expr("element_at(measurements, 1).pressure_psi").alias("pressure_psi")
)

# 4. Load flattened telemetry partitioned by plant region
telemetry_flat_df.write.mode("overwrite").partitionBy("plant_region").parquet("warehouse/industrial/machinery_telemetry")

# 5. Release resources
spark.stop()