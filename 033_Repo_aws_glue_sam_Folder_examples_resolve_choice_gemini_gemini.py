"""Business purpose: Standardize, audit, and route IoT sensor readings containing mixed-type identifiers.
Source: data/landing/iot/sensor_readings.json
Target: Multiple output streams for varying data quality and downstream consumers.
Output grain: One record per sensor reading.
Business rules: Simulate AWS Glue resolveChoice by casting, mapping, structuring, and filtering invalid data types natively.
Generation profile: Portable PySpark script replacing Glue DynamicFrames and resolveChoice methodologies.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("IoTSensorDataQualityETL").getOrCreate()

# 2. Extract raw IoT sensor readings
sensor_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("sensor_id_raw", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("battery_level", IntegerType(), True)
])
sensor_raw_df = spark.read.schema(sensor_schema).json("data/landing/iot/sensor_readings.json")

# 3. Track 1: Standard Cast (Invalid string casts become null)
sensor_cast_df = sensor_raw_df.withColumn("sensor_id", col("sensor_id_raw").cast(LongType()))

# 4. Track 2: Strict Projection (Drop records where the cast to Long fails)
sensor_strict_df = sensor_cast_df.filter(col("sensor_id").isNotNull())

# 5. Track 3: Audit Columns (Retain both original string and casted long explicitly)
sensor_audit_df = sensor_raw_df.withColumn("sensor_id_long", col("sensor_id_raw").cast(LongType())) \
    .withColumnRenamed("sensor_id_raw", "sensor_id_str")

# 6. Track 4: Nested Struct Configuration (Nest both types into a complex record)
sensor_struct_df = sensor_raw_df.withColumn(
    "sensor_id_record",
    struct(
        col("sensor_id_raw").cast(LongType()).alias("as_long"),
        col("sensor_id_raw").alias("as_string")
    )
).drop("sensor_id_raw")

# 7. Track 5: Filtered Alerts (High Temperature anomaly detection)
sensor_alert_df = sensor_raw_df.filter(col("temperature_c") > 30.0)

# 8. Persist all tracks to downstream JSON consumers
sensor_cast_df.write.mode("overwrite").json("warehouse/iot/processed_cast")
sensor_strict_df.write.mode("overwrite").json("warehouse/iot/processed_strict")
sensor_audit_df.write.mode("overwrite").json("warehouse/iot/processed_audit")
sensor_struct_df.write.mode("overwrite").json("warehouse/iot/processed_struct")
sensor_alert_df.write.mode("overwrite").json("warehouse/iot/temperature_alerts")

# 9. Release resources
spark.stop()