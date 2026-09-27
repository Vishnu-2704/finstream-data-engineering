import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(override=True)

postgres_password = os.getenv("POSTGRES_PASSWORD")

if not postgres_password:
    raise RuntimeError("POSTGRES_PASSWORD is missing from .env")

spark = (
    SparkSession.builder
    .appName("FinStream-Historical-Gold-To-PostgreSQL")
    .master("local[*]")
    .config(
        "spark.jars",
        "/usr/share/java/postgresql-42.7.10.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

gold_path = "data/batch_gold/market_metrics"

gold_df = spark.read.parquet(gold_path)

print("Historical Gold records:", gold_df.count())

(
    gold_df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/finstream")
    .option("dbtable", "historical_market_metrics")
    .option("user", "postgres")
    .option("password", postgres_password)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", "1000")
    .mode("overwrite")
    .save()
)

print("Historical Gold data loaded into PostgreSQL.")

spark.stop()
