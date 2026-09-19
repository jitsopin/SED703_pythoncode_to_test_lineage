"""Business purpose: Consolidate medical staff assignments with hospital facility details.
Source: warehouse/medical/staff, warehouse/medical/assignments, warehouse/medical/hospitals
Target: Multiple curated formats (standard, single-file, partitioned) and Operational JDBC database.
Output grain: One record per staff assignment period.
Business rules: Flatten hospital details, join staff histories, and distribute the dataset across different storage formats and systems.
Generation profile: Portable PySpark script replacing AWS Glue Join, Relationalize, and DynamicFrame logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("MedicalStaffHistoryETL").getOrCreate()

# 2. Extract medical staff records
staff_df = spark.read.parquet("warehouse/medical/staff")

# 3. Extract staff assignments
assignments_df = spark.read.parquet("warehouse/medical/assignments")

# 4. Extract hospital facility records
hospitals_df = spark.read.parquet("warehouse/medical/hospitals")

# 5. Clean and project hospital data
hospitals_clean_df = hospitals_df.drop("facility_metadata", "internal_identifiers") \
    .withColumnRenamed("id", "hospital_id") \
    .withColumnRenamed("name", "hospital_name")

# 6. Consolidate staff, assignments, and hospitals (simulating Glue Join and drop_fields)
staff_assignments_df = staff_df.join(assignments_df, staff_df.id == assignments_df.staff_id, "inner") \
    .drop(assignments_df.staff_id)

staff_history_df = staff_assignments_df.join(hospitals_clean_df, staff_assignments_df.organization_id == hospitals_clean_df.hospital_id, "inner") \
    .drop(staff_assignments_df.organization_id) \
    .drop(hospitals_clean_df.hospital_id)

# 7. Write out the history to Standard Parquet
staff_history_df.write.mode("overwrite").parquet("warehouse/medical/staff_history_standard")

# 8. Write out the history to Single-File Parquet
single_file_df = staff_history_df.repartition(1)
single_file_df.write.mode("overwrite").parquet("warehouse/medical/staff_history_single")

# 9. Write out the history to Partitioned Parquet
staff_history_df.write.mode("overwrite").partitionBy("hospital_name").parquet("warehouse/medical/staff_history_partitioned")

# 10. Write flat records to Operational Database (Replacing Glue Relationalize/Redshift Sink)
jdbc_url = "jdbc:postgresql://db-medical-prod.internal:5432/operational_db"
jdbc_properties = {
    "user": "db_admin",
    "password": "secure_password",
    "driver": "org.postgresql.Driver"
}

staff_history_df.write.mode("overwrite").jdbc(
    url=jdbc_url,
    table="public.staff_history_records",
    properties=jdbc_properties
)

# 11. Release resources
spark.stop()