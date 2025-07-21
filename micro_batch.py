import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count, trim, when
from pyspark.sql.types import *

# Tạo thư mục nếu chưa có
os.makedirs("output", exist_ok=True)
os.makedirs("log", exist_ok=True)

spark = SparkSession.builder \
    .appName("ClickstreamAggParquet") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("action", StringType()),
    StructField("url", StringType()),
    StructField("timestamp", StringType())
])

# Đọc Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value thành dataframe có schema
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select(
        col("data.user_id"),
        col("data.session_id"),
        col("data.action"),
        col("data.url"),
        to_timestamp("data.timestamp").alias("event_time")
    )

df_parsed = df_parsed.withColumn("action", trim(when(col("action").isNull(), "UNKNOWN").otherwise(col("action")))) \
                     .withColumn("url", trim(when(col("url").isNull(), "UNKNOWN").otherwise(col("url")))) \

# Ghi dữ liệu từng batch vào output/ dưới dạng Parquet
def write_batch(batch_df, batch_id):
    output_path = f"output/batch_{batch_id}.parquet"
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(output_path)
    print(f"[INFO] Đã ghi parquet: {output_path}")

    print(f"\nBatch {batch_id}:\n")
    batch_df.show(truncate=False)

    log_path = f"log/batch_{batch_id}.log"
    lines = batch_df.rdd.map(lambda row: "\t".join([str(x) for x in row])).collect()
    with open(log_path, "w") as f:
        for line in lines:
            f.write(line + "\n")

    # Aggregation 1: Đếm số lượng mỗi loại action
    agg_action_count = batch_df.groupBy("action").count()
    agg_action_count.write.mode("overwrite").parquet(f"output/aggregation/action_count_batch_{batch_id}.parquet")

    # Aggregation 2: Tổng số hành động theo user_id
    agg_action_per_user = batch_df.groupBy("user_id").count()
    agg_action_per_user.write.mode("overwrite").parquet(f"output/aggregation/action_per_user_batch_{batch_id}.parquet")

# Trigger micro-batch mỗi 10s
df_parsed.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()
 