"""
Business purpose: publish the daily loss summary of the loan book so risk
reporting reads one measured row per branch, product class and jurisdiction.
Source: the table lending_curated.loan_risk_vectors, one row per loan with a
semi-structured pricing payload held as JSON text.
Target: the table lending_curated.daily_loss_summary, appended once per run.
Output grain: one row per report_date, branch, product_class and jurisdiction.
Business rules: only loans of the current snapshot date are measured, exposure is
read from the pricing payload by a rule that differs per product class and falls
back to the stored loss contribution scaled to currency units, the jurisdiction is
derived from the region, expected_loss_99 is the ninety-ninth percentile loss of a
normal daily return with a fifteen basis point standard deviation, and the summary
records the model that produced it.
Generation profile: portable flat batch PySpark risk aggregation.
Runtime arguments: none.
Assumptions: both tables already exist; the pricing payload is JSON text read with
native JSON path functions instead of the Iceberg variant type; expected_loss_99
uses the closed form of the parametric measure the original column and metadata
already name, because the thousand-scenario simulation lived in a decorated
function, which is not allowed here and converges to the same figure; and the
trade amendment and cancellation at the end of the original were a feature
demonstration on the source table, not part of building this summary.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_date, lit
from pyspark.sql.functions import abs as spark_abs, max as spark_max, sum as spark_sum

# 1. Initialize Spark
spark = SparkSession.builder.appName("lending_daily_loss_summary").getOrCreate()

# 2. Read the day's loans and read exposure out of the pricing payload
loan_risk_enriched_df = spark.sql("""
    SELECT
        loan_id, portfolio_id, branch, product_class, region,
        CASE product_class
            WHEN 'MORTGAGE' THEN CAST(get_json_object(pricing_vector, '$.principal') AS DOUBLE)
                                 * CAST(get_json_object(pricing_vector, '$.factors.pd') AS DOUBLE)
            WHEN 'CARD' THEN CAST(get_json_object(pricing_vector, '$.balance_usd') AS DOUBLE) * 0.01
            WHEN 'AUTO' THEN CAST(get_json_object(pricing_vector, '$.residual_value') AS DOUBLE)
            ELSE loss_contribution * 1000000
        END AS risk_exposure,
        get_json_object(pricing_vector, '$.model') AS pricing_model,
        CASE product_class
            WHEN 'MORTGAGE' THEN CAST(get_json_object(pricing_vector, '$.scenarios[0].breakdown.by_segment.prime') AS DOUBLE)
            WHEN 'CARD' THEN CAST(get_json_object(pricing_vector, '$.rate_surface.skew.short_term.1W') AS DOUBLE)
            WHEN 'AUTO' THEN CAST(get_json_object(pricing_vector, '$.model_params.calibration.fit_error') AS DOUBLE)
        END AS deep_nested_metric,
        CASE region
            WHEN 'EMEA' THEN 'UK' WHEN 'AMER' THEN 'US'
            WHEN 'APAC' THEN 'JP' ELSE 'OTHER'
        END AS jurisdiction,
        risk_weight
    FROM lending_curated.loan_risk_vectors
    WHERE snapshot_date = current_date()
""")

# 3. Measure the ninety-ninth percentile daily loss of every loan
loan_risk_scored_df = loan_risk_enriched_df \
    .withColumn("expected_loss_99",
                spark_abs(col("risk_exposure")) * col("risk_weight") * lit(0.015) * lit(2.326348))

# 4. Total the book by branch, product class and jurisdiction
loss_summary_df = loan_risk_scored_df \
    .groupBy("branch", "product_class", "jurisdiction") \
    .agg(count("*").alias("loan_count"),
         spark_sum("risk_exposure").alias("total_exposure"),
         spark_sum("expected_loss_99").alias("total_loss_99"),
         spark_max("expected_loss_99").alias("max_single_loan_loss"))

# 5. Stamp the report date and the model that produced the figures
daily_loss_summary_df = loss_summary_df.select(
    current_date().alias("report_date"),
    col("branch"),
    col("product_class"),
    col("jurisdiction"),
    col("loan_count"),
    col("total_exposure"),
    col("total_loss_99"),
    col("max_single_loan_loss"),
    lit('{"model":"ParametricLoss","confidence":0.99}').alias("report_metadata"))

# 6. Append the run to the summary table
daily_loss_summary_df.writeTo("lending_curated.daily_loss_summary").append()

# 7. Release resources
spark.stop()
