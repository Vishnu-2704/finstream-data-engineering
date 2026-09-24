import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(override=True)
postgres_password = os.getenv("POSTGRES_PASSWORD")

if not postgres_password:
    raise RuntimeError("POSTGRES_PASSWORD is missing from .env")


spark = (
    SparkSession.builder
    .appName("FinStream-Gold-To-PostgreSQL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_df = (
    spark.read
    .parquet("data/gold/market_metrics_compacted")
)

print("Gold records to stage:", gold_df.count())

(
    gold_df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/finstream")
    .option("dbtable", "market_metrics_staging")
    .option("user", "postgres")
    .option("password", postgres_password)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", "1000")
    .mode("overwrite")
    .save()
)

print("Gold data loaded into staging table.")

spark.stop()
