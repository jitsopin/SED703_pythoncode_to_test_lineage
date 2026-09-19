"""Business purpose: Generate synthetic customer demographics for a simulated Data Lake environment.
Source: In-memory distributed sequence generator.
Target: warehouse/datalake/users (JSON format)
Output grain: One record per generated user.
Business rules: Generate 1000 sequential user IDs starting at 1, dynamically derive synthetic demographic attributes (replacing the Faker library), and load into a consolidated JSON storage format.
Generation profile: Portable PySpark script replacing local iteration, single-file writes, and Python Faker dependencies.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("DataLakeSyntheticGeneration").getOrCreate()

# 2. Generate base sequence for 1000 records starting at ID 1
users_raw_df = spark.range(1, 1001).withColumnRenamed("id", "userid")

# 3. Derive synthetic attributes natively (Replacing Python Faker module)
users_derived_df = users_raw_df.select(
    col("userid"),
    expr("concat('Synthetic_User_', userid)").alias("name"),
    expr("cast(rand() * 83 + 18 as int)").alias("age"),
    expr("concat(userid, ' Fake Street')").alias("street"),
    expr("concat('Mock_City_', cast((userid % 50) as string))").alias("city"),
    expr("concat('Mock_State_', cast((userid % 10) as string))").alias("state"),
    expr("cast(90000 + userid as string)").alias("zip")
)

# 4. Load generated customers to distributed Data Lake storage (Replacing single-file JSON dumps)
users_derived_df.write.mode("overwrite").json("warehouse/datalake/users")

# 5. Release resources
spark.stop()