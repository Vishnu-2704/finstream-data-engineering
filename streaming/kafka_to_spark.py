from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# --------------------------------------------------
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FinStream-Kafka-To-Bronze")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Define the Kafka message schema
# --------------------------------------------------

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", StringType(), True)
])


# --------------------------------------------------
# 3. Read streaming data from Kafka
# --------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "market_ticks")
    .option("startingOffsets", "latest")
    .load()
)


# --------------------------------------------------
# 4. Convert Kafka value from binary to string
# --------------------------------------------------

json_stream = raw_stream.select(
    col("value").cast("string").alias("json_data")
)


# --------------------------------------------------
# 5. Parse JSON into structured columns
# --------------------------------------------------

parsed_stream = (
    json_stream
    .select(
        from_json(col("json_data"), schema).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# 6. Convert event_time into timestamp
# --------------------------------------------------

bronze_stream = (
    parsed_stream
    .withColumn(
        "event_time",
        to_timestamp(col("event_time"))
    )
)


# --------------------------------------------------
# 7. Write streaming data to Bronze as Parquet
# --------------------------------------------------

query = (
    bronze_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/bronze/market_ticks")
    .option("checkpointLocation", "checkpoints/market_ticks")
    .trigger(processingTime="30 seconds")
    .start()
)


# --------------------------------------------------
# 8. Keep the streaming application running
# --------------------------------------------------

print("FinStream Bronze streaming job started...")
print("Writing Kafka market_ticks data to Bronze Parquet.")

query.awaitTermination()
