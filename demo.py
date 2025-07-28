from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def main():
   # Khởi tạo SparkSession với Hive support
   spark = (
       SparkSession.builder
           .appName("HiveReadWriteS3")
           .enableHiveSupport()
           .getOrCreate()
   )
   # Cấu hình truy cập S3/MinIO
   #hadoop_conf = spark._jsc.hadoopConfiguration()
   #hadoop_conf.set("fs.s3a.access.key", "minioadmin")
   #hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
   #hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
   #hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
   #hadoop_conf.set("fs.s3a.path.style.access", "true")

   # Liệt kê các database
   print("=== DATABASES ===")
   spark.sql("SHOW DATABASES").show(truncate=False)
   
   # Liệt kê các bảng trong database default
   print("=== TABLES IN default ===")
   spark.sql("USE default")
   spark.sql("SHOW TABLES").show(truncate=False)
   
   # Tạo DataFrame giả lập
   df = spark.createDataFrame([
       (1, "Alice"),
       (2, "Bob"),
       (3, "Charlie")
   ], ["id", "name"])
   
   # Ghi DataFrame ra S3 ở dạng Parquet
   output_path = "s3a://datalake/users"
   print(f"Ghi Parquet vào: {output_path}")
   df.write.mode("overwrite").parquet(output_path)
   
   # Đọc lại dữ liệu đã ghi
   print(f"Đọc lại Parquet từ: {output_path}")
   df_read = spark.read.parquet(output_path)
   df_read.show()
   
   # Dừng Spark
   spark.stop()

if __name__ == "__main__":
   main()