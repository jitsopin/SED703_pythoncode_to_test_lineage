"""
Business purpose: Continuously compute running word counts from incoming text records.
Source: Streaming text files under data/landing/word_count.
Target: Complete-state JSON snapshot at data/curated/word_count/current_counts.
Output grain: One row per distinct word with its current running count.
Important rules: Read text records continuously; split each line on a single space; explode tokens to one row per word; group by word; count occurrences; emit the complete aggregation state; checkpoint streaming progress.
Generation profile: DATAFRAME_CHAIN + SECONDARY_STYLE (STREAMING_WORD_COUNT)
Runtime arguments: None.
Assumptions: The original network socket source and console-only sink are environment-specific. A portable text-file stream preserves line ingestion semantics, and a foreachBatch overwrite JSON snapshot preserves complete-state output semantics without requiring an interactive network endpoint.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

spark = SparkSession.builder.appName("portable_streaming_word_count").getOrCreate()

input_path = "data/landing/word_count"
output_path = "data/curated/word_count/current_counts"
checkpoint_path = "data/curated/word_count/checkpoints/current_counts"

lines_df = (
    spark.readStream
    .format("text")
    .load(input_path)
)

words_df = lines_df.select(
    explode(
        split(col("value"), " ")
    ).alias("word")
)

word_counts_df = words_df.groupBy(
    col("word"),
).count()

snapshot_query = (
    word_counts_df.writeStream
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_path)
    .foreachBatch(
        lambda batch_df, batch_id: (
            batch_df.write
            .mode("overwrite")
            .json(output_path)
        )
    )
    .start()
)

snapshot_query.awaitTermination()

spark.stop()
