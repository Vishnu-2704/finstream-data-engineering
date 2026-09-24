from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    avg,
    min,
    max,
    count,
    round
)

spark = (
    SparkSession.builder
    .appName("FinStream-Silver-To-Gold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_stream = (
    spark.readStream
    .format("parquet")
    .schema("""
        symbol STRING,
        price DOUBLE,
        event_time TIMESTAMP
    """)
    .load("data/silver/market_ticks")
)

gold_stream = (
    silver_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute")
    )
    .agg(
        round(avg("price"), 4).alias("avg_price"),
        round(min("price"), 4).alias("min_price"),
        round(max("price"), 4).alias("max_price"),
        count("*").alias("tick_count")
    )
    .select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("tick_count")
    )
)

query = (
    gold_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/gold/market_metrics_v2")
    .option("checkpointLocation", "checkpoints/gold_market_metrics_v2")
    .trigger(processingTime="30 seconds")
    .start()
)

print("FinStream Gold streaming job started...")
print("Generating finalized 1-minute market metrics.")

query.awaitTermination()
