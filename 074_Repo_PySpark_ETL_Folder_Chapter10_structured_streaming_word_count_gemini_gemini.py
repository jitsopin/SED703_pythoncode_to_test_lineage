"""Business purpose: Parse and aggregate word frequency counts from operational text logs.
Source: data/landing/logs/text_events (Text format)
Target: warehouse/logs/word_counts (Parquet format)
Output grain: One record per unique word with its corresponding total frequency count.
Business rules: Extract raw log lines, split the strings by whitespace, explode the arrays into discrete rows, aggregate the counts, and persist to local enterprise storage.
Generation profile: Portable PySpark micro-batch script replacing ad-hoc socket streaming loops and console output sinks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("LogWordCountETL").getOrCreate()

# 2. Extract raw text logs from the local landing zone (Replacing localhost socket stream)
lines_df = spark.read.text("data/landing/logs/text_events")

# 3. Transform: Split lines into arrays of words and explode into individual rows
words_df = lines_df.select(
    explode(split(col("value"), " ")).alias("word")
)

# 4. Transform: Aggregate to compute total frequencies per word
word_counts_df = words_df.groupBy("word").count()

# 5. Load: Persist the aggregated metrics to the local data warehouse (Replacing console sink)
word_counts_df.write \
    .mode("overwrite") \
    .parquet("warehouse/logs/word_counts")

# 6. Release resources
spark.stop()