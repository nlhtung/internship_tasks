#!/usr/bin/env python3
from pyspark.sql import SparkSession

def main():
    # Khởi SparkSession với Hive support
    spark = (
        SparkSession.builder
            .appName("HiveExtTableOnS3")
            .enableHiveSupport()
            .getOrCreate()
    )

    # 1. Ghi DataFrame lên MinIO với kiểu id là LongType
    df = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        ["id", "name"]
    )
    output_path = "s3a://datalake/users"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Đã ghi data lên: {output_path}")

    # 2. Tạo database nếu chưa có
    spark.sql("CREATE DATABASE IF NOT EXISTS demo")

    # 3. Tạo External Table với id là BIGINT (tương ứng INT64 trên Parquet)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS demo.users (
          id   BIGINT,
          name STRING
        )
        STORED AS PARQUET
        LOCATION 's3a://datalake/users'
    """)
    print("External table demo.users đã được tạo.")

    # 4. Kiểm tra metadata và đọc lại dữ liệu
    spark.sql("SHOW TABLES IN demo").show(truncate=False)
    spark.sql("SELECT * FROM demo.users").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
