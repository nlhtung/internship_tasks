from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

# 1. Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("StructuredStreaming-SCD") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa schema cho dữ liệu JSON
schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("name", StringType()) \
    .add("city", StringType()) \
    .add("gender", StringType()) \
    .add("event_time", TimestampType())

# 3. Bảng target (in-memory)
target_columns = schema.names + ["start_date", "end_date", "is_current"]
target_schema = StructType(schema.fields + [
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("is_current", BooleanType(), True),

])
target_df = spark.createDataFrame([], target_schema)

# 4. Hàm detect record changed trên tất cả các cột (ngoại trừ khoá và thời gian)
def detect_changed_records(source_df, target_df):
    key_cols = ["customer_id"]
    compare_cols = [c for c in source_df.columns if c not in key_cols + ["event_time"]]
    joined = source_df.alias("src").join(
        target_df.filter("is_current = true").alias("tgt"),
        on=key_cols,
        how="inner"
    )
    condition = " OR ".join([f"src.{c} IS DISTINCT FROM tgt.{c}" for c in compare_cols])
    return joined.filter(expr(condition)).select("src.*")

# 5. Hàm xử lý mỗi batch
def process_batch(source_df, batch_id):
    global target_df
    if source_df.rdd.isEmpty():
        return

    source_df = source_df.withColumn("start_date", col("event_time")) \
                         .withColumn("end_date", lit(None).cast("timestamp")) \
                         .withColumn("is_current", lit(True))

    print(f"\n========== BATCH {batch_id} ==========")

    # ===== SCD TYPE 1 =====
    existing_ids = target_df.select("customer_id").distinct()
    to_overwrite = source_df.join(existing_ids, on="customer_id", how="inner")
    to_insert = source_df.join(existing_ids, on="customer_id", how="left_anti")
    target_df = to_overwrite.unionByName(to_insert).dropDuplicates(["customer_id"])
    print("== SCD Type 1 ==")
    target_df.orderBy("customer_id").show(truncate=False)

    # ===== SCD TYPE 2 =====
    changed_df = detect_changed_records(source_df, target_df)
    changed_ids = changed_df.select("customer_id").distinct()

    expired_df = target_df.join(changed_ids, "customer_id") \
        .withColumn("end_date", changed_df.select("event_time").first()["event_time"]) \
        .withColumn("is_current", lit(False))

    new_versions = changed_df.withColumn("start_date", col("event_time")) \
                             .withColumn("end_date", lit(None).cast("timestamp")) \
                             .withColumn("is_current", lit(True))

    not_changed = target_df.join(changed_ids, on="customer_id", how="left_anti")
    target_df = not_changed.unionByName(expired_df).unionByName(new_versions)

    print("== SCD Type 2 ==")
    target_df.orderBy("customer_id", "start_date").show(truncate=False)

    # ===== SCD TYPE 4 =====
    history_df = detect_changed_records(source_df, target_df)
    history_result = target_df.join(history_df.select("customer_id").distinct(), "customer_id") \
        .select("customer_id", "name", "city", "gender", col("start_date").alias("archived_at"))
    print("== SCD Type 4 - History ==")
    history_result.orderBy("customer_id", "archived_at").show(truncate=False)

# 6. Read từ Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "scd") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 7. Streaming: xử lý theo batch
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_scd_structured") \
    .start()

query.awaitTermination(600)  # chạy trong 10 phút
query.stop()
spark.stop()