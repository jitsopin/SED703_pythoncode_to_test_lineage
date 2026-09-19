"""
Business purpose: Validate a portable JDBC source connection, assert expected schema/count, export the read dataset to JSON, and validate a JDBC sink with a one-row test record.
Sources: JDBC query over portable employee/account-style data.
Targets: JSON validation export at data/curated/connector_validation/source_snapshot and JDBC table validation.account_sink.
Output grain: Source branch preserves one row per JDBC query result; sink branch contains one synthetic validation row.
Important rules: Read NumberOfEmployees and CreatedDate, assert INTEGER/TIMESTAMP schema, assert expected_count=2, write source snapshot as JSON, create a one-row Name record, and write it to the JDBC target.
Generation profile: DATAFRAME_CHAIN + JDBC_VALIDATION_PORT
Runtime arguments: Optional source JDBC URL, source user, source password, target JDBC URL, target user, and target password.
Assumptions: Compatible JDBC drivers are available; source query returns exactly two rows with the expected schema.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

spark = SparkSession.builder.appName("portable_jdbc_validation_pipeline").getOrCreate()

source_jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/source_db"
source_jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "source_user"
source_jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"
target_jdbc_url = sys.argv[4] if len(sys.argv) > 4 else "jdbc:postgresql://localhost:5432/target_db"
target_jdbc_user = sys.argv[5] if len(sys.argv) > 5 else "target_user"
target_jdbc_password = sys.argv[6] if len(sys.argv) > 6 else "change_me"

source_query = "(SELECT employee_count, created_at FROM account_source) AS source_query"
source_snapshot_path = "data/curated/connector_validation/source_snapshot"
target_table = "validation.account_sink"

expected_schema = StructType([
    StructField("employee_count", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
])
expected_count = 2

source_records_df = (
    spark.read
    .format("jdbc")
    .option("url", source_jdbc_url)
    .option("dbtable", source_query)
    .option("user", source_jdbc_user)
    .option("password", source_jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

validated_source_df = source_records_df.select(
    "employee_count",
    "created_at",
)

assert validated_source_df.schema == expected_schema
validated_count = validated_source_df.count()
assert validated_count == expected_count

validated_source_df.write.mode("overwrite").json(source_snapshot_path)

sink_test_schema = StructType([
    StructField("name", StringType(), True),
])

sink_test_df = spark.createDataFrame(
    [("Andrew",)],
    sink_test_schema,
)

(
    sink_test_df.write
    .format("jdbc")
    .option("url", target_jdbc_url)
    .option("dbtable", target_table)
    .option("user", target_jdbc_user)
    .option("password", target_jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

spark.stop()
