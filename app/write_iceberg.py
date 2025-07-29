from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
if __name__ == "__main__":
   spark = SparkSession.builder.appName("WriteIceberg").getOrCreate()
   # Tạo DataFrame mẫu
   schema = StructType([
       StructField("id",   IntegerType(), nullable=False),
       StructField("name", StringType(),  nullable=False),
       StructField("age",  IntegerType(), nullable=True)
   ])
   data = [
       (1, "Nguyễn Văn A", 30),
       (2, "Trần Thị B",   25),
       (3, "Lê Văn C",     28)
   ]
   df = spark.createDataFrame(data, schema)
   # Tạo namespace nếu chưa có
   spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_catalog.default")
   # Tạo table Iceberg
   spark.sql("""
     CREATE TABLE IF NOT EXISTS hive_catalog.default.users (
       id   INT,
       name STRING,
       age  INT
     ) USING iceberg
   """)
   # Ghi dữ liệu
   df.writeTo("hive_catalog.default.users").append()
   # Đọc và hiển thị
   spark.table("hive_catalog.default.users").show(truncate=False)
   spark.stop()