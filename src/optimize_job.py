from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Tạo SparkSession
spark = SparkSession.builder.appName("OptimizeSparkJoinJob").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# Đọc dữ liệu sales
sales_df = spark.read.option("header", True).option("inferSchema", True).csv("input/sales_data.csv")

# Đọc dữ liệu promotions
promo_df = spark.read.option("header", True).option("inferSchema", True).csv("input/promotions.csv")

# Join 2 bảng theo PRODUCTCODE
joined_df = sales_df.join(promo_df, on="PRODUCTCODE", how="left")

# Tính doanh thu
joined_df = joined_df.withColumn("CALC_SALES", col("QUANTITYORDERED") * col("PRICEEACH"))

# Lọc các đơn hàng có doanh thu > 3000
filtered_df = joined_df.filter(col("CALC_SALES") > 3000)

# Ghi execution plan bằng explain() cho joined_df
with open("log/joined_df_explain.txt", "w") as f:
    f.write("=== Execution Plan for joined_df ===\n")
    f.write(joined_df._sc._jvm.PythonSQLUtils.explainString(joined_df._jdf.queryExecution(), "formatted"))

# Ghi execution plan cho filtered_df
with open("log/filtered_df_explain.txt", "w") as f:
    f.write("=== Execution Plan for filtered_df ===\n")
    f.write(filtered_df._sc._jvm.PythonSQLUtils.explainString(filtered_df._jdf.queryExecution(), "formatted"))

# Ghi dữ liệu output ra file CSV
filtered_df.select("ORDERNUMBER", "PRODUCTCODE", "PROMOTION_NAME", "CALC_SALES") \
    .coalesce(1).write.mode("overwrite").option("header", True).csv("output/filtered_joined_sales")

spark.stop()
