"""
Business purpose: Build a portable membership-history dataset by joining person, membership, and organization reference data, then publish file and relational outputs.
Sources: Parquet datasets under data/landing/public_service/person_records, membership_records, and organization_records.
Targets: Three Parquet outputs under data/curated/public_service/ and PostgreSQL table analytics.membership_history_flat.
Output grain: One row per matched person-membership-organization record.
Important rules: Remove organization aliases/identifiers, rename organization identifiers/names, join persons to memberships on person id, join organizations on organization id, drop duplicate join keys, write regular/single-file/partitioned Parquet outputs, flatten the history for JDBC, and overwrite the relational target.
Generation profile: DATAFRAME_CHAIN + JOIN_RELATIONALIZE_PORT
Runtime arguments: Optional JDBC URL, JDBC user, and JDBC password are accepted as the first three command-line arguments.
Assumptions: Source datasets contain the referenced identifiers and a PostgreSQL JDBC driver is available for the relational branch.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("portable_membership_history_pipeline").getOrCreate()

jdbc_url = sys.argv[1] if len(sys.argv) > 1 else "jdbc:postgresql://localhost:5432/analytics"
jdbc_user = sys.argv[2] if len(sys.argv) > 2 else "analytics_user"
jdbc_password = sys.argv[3] if len(sys.argv) > 3 else "change_me"

persons_path = "data/landing/public_service/person_records"
memberships_path = "data/landing/public_service/membership_records"
organizations_path = "data/landing/public_service/organization_records"

history_path = "data/curated/public_service/membership_history"
single_path = "data/curated/public_service/membership_history_single"
partitioned_path = "data/curated/public_service/membership_history_partitioned"
jdbc_target_table = "analytics.membership_history_flat"

persons_df = spark.read.parquet(persons_path)
memberships_df = spark.read.parquet(memberships_path)
organizations_raw_df = spark.read.parquet(organizations_path)

organizations_df = (
    organizations_raw_df
    .drop("alternate_names", "external_identifiers")
    .withColumnRenamed("organization_key", "org_id")
    .withColumnRenamed("organization_label", "org_name")
)

person_membership_df = persons_df.join(
    memberships_df,
    persons_df.person_key == memberships_df.person_id,
    "inner",
)

membership_history_df = (
    person_membership_df
    .join(
        organizations_df,
        person_membership_df.organization_id == organizations_df.org_id,
        "inner",
    )
    .drop("person_id", "org_id")
)

membership_history_df.write.mode("overwrite").parquet(history_path)

single_history_df = membership_history_df.repartition(1)
single_history_df.write.mode("overwrite").parquet(single_path)

membership_history_df.write.mode("overwrite").partitionBy("org_name").parquet(
    partitioned_path
)

flat_history_df = membership_history_df.select(
    *[
        col(field.name).alias(field.name.replace(".", "_"))
        for field in membership_history_df.schema.fields
    ]
)

(
    flat_history_df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", jdbc_target_table)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

spark.stop()
