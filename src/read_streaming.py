from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Tạo Spark session
spark = SparkSession.builder.appName("ReadFileStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema tường minh để tránh đọc lỗi trong streaming
schema = "OrderID INT, Date STRING, CustomerID STRING, Product STRING, Quantity INT, Price DOUBLE"

# Đọc dữ liệu từ thư mục stream_input theo chế độ append
stream_df = spark.readStream \
    .option("header", True) \
    .schema(schema) \
    .csv("input/stream_input")

# Tính Revenue
result_df = stream_df.withColumn("Revenue", col("Quantity") * col("Price"))

# Ghi log schema & plan
with open("log/streaming_schema.txt", "w") as f:
    f.write(str(result_df.schema))

with open("log/streaming_explain.txt", "w") as f:
    f.write("=== Physical Execution Plan ===\n")
    f.write(result_df._sc._jvm.PythonSQLUtils.explainString(result_df._jdf.queryExecution(), "formatted"))

# Ghi kết quả ra file CSV (chế độ append)
query = result_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/stream_result") \
    .option("checkpointLocation", "log/checkpoint_streaming") \
    .option("header", True) \
    .start()

query.awaitTermination()
