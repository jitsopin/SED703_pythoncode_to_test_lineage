"""Business purpose: Archive Spark metadata for logistics and supply chain analytics.
Source: Current PySpark Catalog (simulating source operational metadata)
Target 1 (File Mode): warehouse/metadata_archive/ (JSON format)
Target 2 (JDBC Mode): Operational JDBC Metastore Tracking Database
Output grain: One record per metadata entity (databases, tables, columns/partitions).
Business rules: Reflect operational catalog metadata, map into explicit schemas, and route output based on defined operational mode.
Generation profile: Flat portable PySpark script; AWS Glue contexts, DataCatalog connections, and proprietary arguments removed.
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("LogisticsMetadataArchive").getOrCreate()

# Simulated operational arguments
EXECUTION_MODE = "to-s3" # Options: 'to-s3', 'to-jdbc'
S3_OUTPUT_PATH = "warehouse/metadata_archive/"
JDBC_URL = "jdbc:postgresql://db-metadata-prod.internal:5432/metadata_db"
JDBC_PROPS = {"user": "admin", "password": "secure_password", "driver": "org.postgresql.Driver"}

# 2. Extract operational databases via standard Spark Catalog
db_list = [db.name for db in spark.catalog.listDatabases()]
db_schema = StructType([StructField("database_name", StringType(), False)])
db_df = spark.createDataFrame([Row(database_name=db) for db in db_list], db_schema)

# 3. Extract table metadata across discovered databases
table_records = []
for db in db_list:
    try:
        tables = spark.catalog.listTables(db)
        for tbl in tables:
            table_records.append(Row(database_name=db, table_name=tbl.name, table_type=tbl.tableType))
    except Exception:
        pass # Skip unreadable catalogs for simulation
table_schema = StructType([
    StructField("database_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("table_type", StringType(), True)
])
table_df = spark.createDataFrame(table_records, table_schema)

# 4. Extract granular column/partition metadata
column_records = []
for row in table_records:
    try:
        columns = spark.catalog.listColumns(row.table_name, row.database_name)
        for col_meta in columns:
            column_records.append(Row(
                table_name=row.table_name, 
                column_name=col_meta.name, 
                data_type=col_meta.dataType, 
                is_partition=col_meta.isPartition
            ))
    except Exception:
        pass
column_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), True),
    StructField("is_partition", StringType(), True)
])
column_df = spark.createDataFrame(column_records, column_schema)

# 5. Route metadata output based on execution mode
if EXECUTION_MODE == "to-s3":
    # 6a. Archive metadata to file storage
    db_df.write.mode("overwrite").json(f"{S3_OUTPUT_PATH}databases")
    table_df.write.mode("overwrite").json(f"{S3_OUTPUT_PATH}tables")
    column_df.write.mode("overwrite").json(f"{S3_OUTPUT_PATH}columns_and_partitions")

elif EXECUTION_MODE == "to-jdbc":
    # 6b. Sync metadata to relational metastore
    db_df.write.mode("overwrite").jdbc(url=JDBC_URL, table="public.ext_databases", properties=JDBC_PROPS)
    table_df.write.mode("overwrite").jdbc(url=JDBC_URL, table="public.ext_tables", properties=JDBC_PROPS)
    column_df.write.mode("overwrite").jdbc(url=JDBC_URL, table="public.ext_columns", properties=JDBC_PROPS)

# 7. Release resources
spark.stop()