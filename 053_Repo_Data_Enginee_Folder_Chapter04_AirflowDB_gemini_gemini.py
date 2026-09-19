"""Business purpose: Synchronize user demographic profiles to a search index.
Source: Operational PostgreSQL Database (JDBC) - users table
Target: Elasticsearch Index (frompostgresql)
Output grain: One document per user profile.
Business rules: Extract user names and cities from the operational database and index them into Elasticsearch for fast text search without staging intermediate local files.
Generation profile: Portable PySpark script replacing Airflow DAGs, Pandas, psycopg2, and local CSV intermediate storage.
"""

from pyspark.sql import SparkSession

# 1. Initialize SparkSession with Elasticsearch configurations
spark = SparkSession.builder \
    .appName("UserProfileSearchSync") \
    .config("es.nodes", "localhost") \
    .config("es.port", "9200") \
    .getOrCreate()

# 2. Extract user profiles from PostgreSQL via standard JDBC
jdbc_url = "jdbc:postgresql://localhost:5432/dataengineering"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Extract required fields natively, dropping the need for intermediate pandas DataFrames
query = "(SELECT name, city FROM users) AS tmp"
users_df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)

# 3. Load user profiles directly into Elasticsearch (Replacing row-by-row dict indexing)
users_df.write \
    .format("org.elasticsearch.spark.sql") \
    .mode("append") \
    .save("frompostgresql/doc")

# 4. Release resources
spark.stop()