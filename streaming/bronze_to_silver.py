from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


# --------------------------------------------------
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FinStream-Bronze-To-Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Read Bronze streaming data
# --------------------------------------------------

bronze_stream = (
    spark.readStream
    .format("parquet")
    .schema("""
        symbol STRING,
        price DOUBLE,
        event_time TIMESTAMP
    """)
    .load("data/bronze/market_ticks")
)


# --------------------------------------------------
# 3. Clean and validate the data
# --------------------------------------------------

silver_stream = (
    bronze_stream
    .filter(col("symbol").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("price") > 0)
    .filter(col("event_time").isNotNull())
    .withColumn("symbol", col("symbol").cast("string"))
    .withColumn("price", col("price").cast("double"))
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )
)


# --------------------------------------------------
# 4. Write Silver data as Parquet
# --------------------------------------------------

query = (
    silver_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/silver/market_ticks")
    .option(
        "checkpointLocation",
        "checkpoints/silver_market_ticks"
    )
    .trigger(processingTime="30 seconds")
    .start()
)


# --------------------------------------------------
# 5. Keep streaming job alive
# --------------------------------------------------

print("FinStream Silver streaming job started...")
print("Cleaning Bronze market data and writing Silver Parquet.")

query.awaitTermination()
