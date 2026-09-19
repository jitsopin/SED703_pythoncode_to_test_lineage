"""
Business purpose: Build a portable daily trade-risk summary with deterministic scenario-based VaR and produce an amended trade snapshot.
Source: Parquet trade-risk vectors at data/landing/risk_analytics/trade_risk_vectors.
Targets: Appended daily risk summaries at data/curated/risk_analytics/daily_risk_summary and an amended trade snapshot at data/curated/risk_analytics/trade_risk_vectors_amended.
Output grain: Summary output is one row per report_date, desk, asset_class, and jurisdiction; amended trade output is one row per retained trade.
Important rules: Prefer today's trades and fall back to all dates when today is empty, derive asset-class-specific risk exposure from JSON pricing payloads, calculate deterministic 1,000-scenario 99th-percentile VaR, aggregate risk metrics, append the summary, amend the first trade, cancel the last trade, and overwrite the amended snapshot.
Generation profile: DATAFRAME_CHAIN + PORTABLE_BATCH_VAR_FLOW
Runtime arguments: None.
Assumptions: The pricing payload is valid JSON text; portable Spark JSON functions and higher-order SQL expressions are available.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    current_date,
    expr,
    get_json_object,
    lit,
    max as spark_max,
    row_number,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

# 1. Initialize Spark
spark = SparkSession.builder.appName("portable_batch_trade_var_pipeline").getOrCreate()

source_path = "data/landing/risk_analytics/trade_risk_vectors"
summary_target_path = "data/curated/risk_analytics/daily_risk_summary"
amended_target_path = "data/curated/risk_analytics/trade_risk_vectors_amended"

trade_schema = StructType([
    StructField("trade_id", StringType(), False),
    StructField("book_id", StringType(), True),
    StructField("desk", StringType(), True),
    StructField("asset_class", StringType(), True),
    StructField("region", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("pricing_payload_json", StringType(), True),
    StructField("risk_weight", DoubleType(), True),
    StructField("var_contribution", DoubleType(), True),
])

# 2. Extract
trade_raw_df = spark.read.schema(trade_schema).parquet(source_path)

# 3. Identify whether today's rows exist
today_trade_df = trade_raw_df.filter(
    col("trade_date") == current_date().cast("string")
)
today_trade_count = today_trade_df.count()

# 4. Select today's rows when available; otherwise use all rows
selected_trade_df = today_trade_df if today_trade_count > 0 else trade_raw_df

# 5. Derive risk metrics from JSON payloads
trade_enriched_df = selected_trade_df.select(
    col("trade_id"),
    col("book_id"),
    col("desk"),
    col("asset_class"),
    col("region"),
    when(
        col("asset_class") == lit("EQUITY"),
        get_json_object(col("pricing_payload_json"), "$.notional").cast("double")
        * get_json_object(col("pricing_payload_json"), "$.greeks.delta").cast("double"),
    ).when(
        col("asset_class") == lit("FX"),
        get_json_object(col("pricing_payload_json"), "$.notional_usd").cast("double") * lit(0.01),
    ).when(
        col("asset_class") == lit("RATES"),
        get_json_object(col("pricing_payload_json"), "$.dv01").cast("double"),
    ).otherwise(
        col("var_contribution") * lit(1000000.0)
    ).alias("risk_exposure"),
    get_json_object(col("pricing_payload_json"), "$.model").alias("pricing_model"),
    when(
        col("asset_class") == lit("EQUITY"),
        get_json_object(
            col("pricing_payload_json"),
            "$.scenarios[0].breakdown.by_sector.financials",
        ).cast("double"),
    ).when(
        col("asset_class") == lit("FX"),
        get_json_object(
            col("pricing_payload_json"),
            "$.vol_surface.skew.short_term.1W",
        ).cast("double"),
    ).when(
        col("asset_class") == lit("RATES"),
        get_json_object(
            col("pricing_payload_json"),
            "$.model_params.calibration.fit_error",
        ).cast("double"),
    ).alias("deep_nested_metric"),
    when(col("region") == lit("EMEA"), lit("UK"))
    .when(col("region") == lit("AMER"), lit("US"))
    .when(col("region") == lit("APAC"), lit("JP"))
    .otherwise(lit("OTHER"))
    .alias("jurisdiction"),
    col("risk_weight"),
)

# 6. Calculate deterministic 1,000-scenario 99th-percentile VaR without a Python UDF
trade_var_df = trade_enriched_df.withColumn(
    "parametric_var_99",
    expr(
        """
        percentile_approx(
          transform(
            sequence(1, 1000),
            i -> abs(risk_exposure) * risk_weight * 0.015 *
                 sqrt(-2.0 * log(
                   greatest(
                     pmod(abs(hash(concat(trade_id, ':u1:', cast(i as string)))), 999983) / 999983.0,
                     0.000001
                   )
                 )) *
                 cos(
                   2.0 * pi() *
                   pmod(abs(hash(concat(trade_id, ':u2:', cast(i as string)))), 999979) / 999979.0
                 )
          ),
          0.99
        )
        """
    ),
)

# 7. Aggregate daily risk summary
risk_summary_df = trade_var_df.groupBy(
    "desk",
    "asset_class",
    "jurisdiction",
).agg(
    count("*").alias("trade_count"),
    spark_sum("risk_exposure").alias("total_exposure"),
    spark_sum("parametric_var_99").alias("total_var_99"),
    spark_max("parametric_var_99").alias("max_single_trade_var"),
)

# 8. Add report metadata
published_risk_summary_df = risk_summary_df.select(
    current_date().alias("report_date"),
    col("desk"),
    col("asset_class"),
    col("jurisdiction"),
    col("trade_count"),
    col("total_exposure"),
    col("total_var_99"),
    col("max_single_trade_var"),
    lit('{"model":"ParametricVaR","confidence":0.99,"engine":"PortableSpark"}').alias(
        "report_metadata_json"
    ),
)

# 9. Append daily summary
published_risk_summary_df.write.mode("append").parquet(summary_target_path)

# 10. Rank source trades for amendment and cancellation
ascending_trade_spec = Window.orderBy(col("trade_id").asc())
descending_trade_spec = Window.orderBy(col("trade_id").desc())

trade_ranked_df = trade_raw_df.withColumn(
    "amend_rank",
    row_number().over(ascending_trade_spec),
).withColumn(
    "cancel_rank",
    row_number().over(descending_trade_spec),
)

# 11. Apply amendment to the first trade and cancel the last trade
amended_trade_df = trade_ranked_df.filter(
    col("cancel_rank") != 1
).select(
    col("trade_id"),
    col("book_id"),
    col("desk"),
    col("asset_class"),
    col("region"),
    col("trade_date"),
    col("pricing_payload_json"),
    when(col("amend_rank") == 1, lit(2.5))
    .otherwise(col("risk_weight"))
    .alias("risk_weight"),
    when(col("amend_rank") == 1, lit(0.025))
    .otherwise(col("var_contribution"))
    .alias("var_contribution"),
)

# 12. Write amended trade snapshot
amended_trade_df.write.mode("overwrite").parquet(amended_target_path)

# 13. Release resources
spark.stop()
