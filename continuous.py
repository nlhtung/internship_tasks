from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("ClickstreamConsoleContinuous") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("action", StringType()),
    StructField("url", StringType()),
    StructField("timestamp", StringType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select(
        col("data.user_id"),
        col("data.session_id"),
        col("data.action"),
        col("data.url"),
        to_timestamp("data.timestamp").alias("event_time")
    )

df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(continuous="1 second") \
    .start() \
    .awaitTermination()
