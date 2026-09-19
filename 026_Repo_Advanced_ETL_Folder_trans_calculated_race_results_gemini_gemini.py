"""Business purpose: Calculate and reconcile employee performance bonuses based on monthly evaluation rankings.
Source: hr_processed.monthly_evaluations, hr_processed.employees, hr_processed.departments, hr_processed.evaluation_cycles
Target: hr_presentation.calculated_bonus_results
Output grain: One record per employee per evaluation cycle.
Business rules: Filter top 10 rankings, calculate bonus allocation, and merge with historical results keeping the latest update.
Generation profile: Flat portable PySpark script, Delta merge replaced with PySpark window-based reconciliation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, lit, row_number
from pyspark.sql.window import Window

# 1. Initialize SparkSession and parameters
spark = SparkSession.builder.appName("EmployeeBonusCalculation").getOrCreate()
v_batch_date = "2026-09-01"

# 2. Extract monthly evaluations
evaluations_df = spark.table("hr_processed.monthly_evaluations")

# 3. Extract employee dimensions
employees_df = spark.table("hr_processed.employees")

# 4. Extract department dimensions
departments_df = spark.table("hr_processed.departments")

# 5. Extract evaluation cycles
cycles_df = spark.table("hr_processed.evaluation_cycles")

# 6. Filter top evaluations for the current batch
top_evaluations_df = evaluations_df.filter(
    (col("rank_score") <= 10) & (col("batch_date") == lit(v_batch_date))
)

# 7. Join facts with dimensions to build comprehensive bonus records
joined_evaluations_df = top_evaluations_df.join(employees_df, "employee_id", "inner") \
    .join(departments_df, "department_id", "inner") \
    .join(cycles_df, "cycle_id", "inner")

# 8. Derive calculated bonus and format target schema
new_bonus_df = joined_evaluations_df.select(
    col("evaluation_year"), col("department_name"), col("employee_id"),
    col("employee_name"), col("cycle_id"), col("rank_score"),
    col("base_points"), (lit(11) - col("rank_score")).alias("calculated_bonus")
).withColumn("updated_date", current_timestamp())

# 9. Extract historical target snapshot for reconciliation
history_bonus_df = spark.table("hr_presentation.calculated_bonus_results")

# 10. Union historical and new records
combined_bonus_df = history_bonus_df.unionByName(new_bonus_df, allowMissingColumns=True)

# 11. Deduplicate records keeping the most recent update
reconciliation_window = Window.partitionBy("employee_id", "cycle_id").orderBy(desc("updated_date"))
reconciled_bonus_df = combined_bonus_df.withColumn("latest_flag", row_number().over(reconciliation_window)) \
    .filter(col("latest_flag") == 1).drop("latest_flag")

# 12. Persist reconciled bonus results
reconciled_bonus_df.write.mode("overwrite").format("parquet").saveAsTable("hr_presentation.calculated_bonus_results")

# 13. Release resources
spark.stop()