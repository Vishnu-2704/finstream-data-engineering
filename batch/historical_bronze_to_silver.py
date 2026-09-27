from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = (
    SparkSession.builder
    .appName("FinStream-Historical-Bronze-To-Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_path = "data/batch_bronze/market_history"
silver_path = "data/batch_silver/market_history"

bronze_df = (
    spark.read
    .option("header", True)
    .csv(bronze_path)
)

silver_df = (
    bronze_df
    .withColumn("datetime", to_timestamp(col("datetime")))
    .withColumn("open", col("open").cast("double"))
    .withColumn("high", col("high").cast("double"))
    .withColumn("low", col("low").cast("double"))
    .withColumn("close", col("close").cast("double"))
    .withColumn("volume", col("volume").cast("long"))
    .filter(col("symbol").isNotNull())
    .filter(col("datetime").isNotNull())
    .filter(col("close").isNotNull())
    .filter(col("close") > 0)
    .filter(col("volume").isNotNull())
    .filter(col("volume") >= 0)
)

(
    silver_df
    .write
    .mode("overwrite")
    .parquet(silver_path)
)

print(f"Bronze records: {bronze_df.count()}")
print(f"Silver records: {silver_df.count()}")
print(f"Silver data written to: {silver_path}")

spark.stop()
