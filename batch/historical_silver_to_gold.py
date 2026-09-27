from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    min,
    max,
    sum,
    count,
    round,
    window
)

spark = (
    SparkSession.builder
    .appName("FinStream-Historical-Silver-To-Gold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_path = "data/batch_silver/market_history"
gold_path = "data/batch_gold/market_metrics"

silver_df = spark.read.parquet(silver_path)

gold_df = (
    silver_df
    .groupBy(
        col("symbol"),
        window(col("datetime"), "1 minute")
    )
    .agg(
        round(avg("close"), 4).alias("avg_close_price"),
        round(min("low"), 4).alias("min_price"),
        round(max("high"), 4).alias("max_price"),
        round(sum("volume"), 0).alias("total_volume"),
        count("*").alias("tick_count")
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_close_price"),
        col("min_price"),
        col("max_price"),
        col("total_volume"),
        col("tick_count")
    )
)

(
    gold_df
    .write
    .mode("overwrite")
    .parquet(gold_path)
)

print(f"Silver records: {silver_df.count()}")
print(f"Gold windows: {gold_df.count()}")
print(f"Gold data written to: {gold_path}")

spark.stop()
