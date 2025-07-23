from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# 1. Khởi tạo SparkSession
spark = SparkSession.builder.appName("SCD_Implementation_All").getOrCreate()

# 2. Dữ liệu gốc (dimension hiện tại)
target_data = [
    (1, "Nguyễn Văn A", "HCM", "Nam", "2025-01-01", None, True),
    (2, "Trần Thị B", "Đà Nẵng", "Nữ", "2025-01-01", None, True),
    (4, "Lê D", "Thanh Hoá", "Nữ", "2025-03-06", None, True),
    (5, "Võ Thị E", "Huế", "Nam", "2019-01-01", None, True)
]

target_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("is_current", BooleanType(), True)
])

target_df = spark.createDataFrame(target_data, schema=target_schema)

# 3. Batch mới
source_data = [
    (1, "Nguyễn Văn A", "Hà Nội", "Nam"),
    (2, "Trần Thị B", "Đà Nẵng", "Nữ"),
    (3, "Lê Văn C", "Hải Phòng", "Nam"),
    (4, "Lê Thị D", "Thanh Hoá", "Nữ"),
    (5, "Võ E", "","Nữ")
]

source_df = spark.createDataFrame(source_data, ["customer_id", "name", "city", "gender"])

# ============ SCD TYPE 1 ============
scd1_df = source_df.alias("src").join(
    target_df.alias("tgt"), on="customer_id", how="left"
).select(
    col("src.customer_id"),
    col("src.name"),
    col("src.city"),
    col("src.gender"),
    current_date().alias("start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

scd1_df.show()
print("=== SCD TYPE 1 ===")

# ============ SCD TYPE 2 ============
metadata_cols = {"start_date", "end_date", "is_current"}
compare_cols = [c for c in source_df.columns if c != "customer_id" and c in target_df.columns and c not in metadata_cols]

change_expr = " OR ".join([f"(src.`{c}` IS DISTINCT FROM tgt.`{c}`)" for c in compare_cols])

new_records = source_df.alias("src").join(
    target_df.filter("is_current = true").alias("tgt"), on="customer_id", how="left"
).filter(expr(change_expr)).select(
    col("src.customer_id"), col("src.name"), col("src.city"), col("src.gender"),
    current_date().alias("start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

expired_records = target_df.filter("is_current = true").alias("tgt").join(
    source_df.alias("src"), on="customer_id"
).filter(expr(change_expr)).select(
    col("tgt.customer_id"), col("tgt.name"), col("tgt.city"), col("tgt.gender"),
    col("tgt.start_date"),
    current_date().alias("end_date"),
    lit(False).alias("is_current")
)

unchanged_records = target_df.alias("tgt").join(
    new_records.select("customer_id").alias("new"), on="customer_id", how="left_anti"
)

final_scd2_df = unchanged_records.unionByName(expired_records).unionByName(new_records)
final_scd2_df.orderBy("customer_id", "start_date").show()
print("=== SCD TYPE 2 ===")

# ============ SCD TYPE 4 ============
history_df = target_df.filter("is_current = true").alias("tgt").join(
    source_df.alias("src"), on="customer_id"
).filter(expr(change_expr)).select(
    col("tgt.customer_id"), col("tgt.name"), col("tgt.city"), col("tgt.gender"),
    current_timestamp().alias("archived_at")
)

main_df_scd4 = scd1_df
history_df.show()
print("=== SCD TYPE 4 - HISTORY TABLE ===")

main_df_scd4.show()
print("=== SCD TYPE 4 - MAIN TABLE ===")
 