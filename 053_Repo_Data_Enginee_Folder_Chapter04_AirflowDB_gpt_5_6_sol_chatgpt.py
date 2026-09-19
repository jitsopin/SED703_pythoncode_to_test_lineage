"""
Business purpose: Extract user name and city attributes from a relational source, materialize a portable intermediate dataset, and publish the records to a generic searchable sink dataset.
Source: JDBC table app.users.
Intermediate target: CSV dataset at data/curated/airflow_db/users_extract.
Final target: JSON dataset at data/curated/airflow_db/search_index/from_relational.
Output grain: One row per source user record containing name and city.
Important rules: Select only name and city from the relational source; preserve the intermediate materialization step; publish the same projected records to the final searchable-sink dataset.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (RELATIONAL_TO_SEARCH_SINK)
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password.
Assumptions: Scheduler/retry metadata is orchestration rather than row-level ETL and is omitted; the original search-client indexing behavior is represented portably as a JSON sink because no portable endpoint/configuration is present in the source.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("portable_relational_to_search_sink").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/app"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "app_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

source_table = "app.users"
extract_path = "data/curated/airflow_db/users_extract"
search_sink_path = "data/curated/airflow_db/search_index/from_relational"

users_raw_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", source_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

users_selected_df = users_raw_df.select(
    col("name"),
    col("city"),
)

users_selected_df.write.mode("overwrite").option("header", True).csv(extract_path)

users_search_df = users_selected_df.select(
    col("name"),
    col("city"),
)

users_search_df.write.mode("overwrite").json(search_sink_path)

spark.stop()
