from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FinStream-Gold-CSV-Export")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet("data/gold/market_metrics_compacted")

(
    df
    .orderBy("window_start")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "false")
    .csv("data/gold/market_metrics_csv")
)

spark.stop()

print("Gold data exported successfully.")
