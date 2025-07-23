from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DataType, BooleanType

# 1. Initialize SparkSession
spark = SparkSession.builder.appName("SCD_Implementation_All").getOrCreate()

# 2. Sample Data
# Existing target dimension table
target_data = [
    (1, "Nguyễn Văn A", "HCM", "2025-01-01", None, True),
    (2, "Trần Thị B", "Đà Nẵng", "2025-01-01", None, True)
]

target_schema = StructType([
   StructField("customer_id", IntegerType(), True),
   StructField("name", StringType(), True),
   StructField("city", StringType(), True),
   StructField("start_date", StringType(), True),   # tạm thời dùng StringType
   StructField("end_date", StringType(), True),     # tạm thời dùng StringType
   StructField("is_current", BooleanType(), True)
])

target_df = spark.createDataFrame(target_data, schema=target_schema)

# New incoming batch from source
source_data = [
    (1, "Nguyễn Văn A", "Hà Nội"),
    (2, "Trần Thị B", "Đà Nẵng"),
    (3, "Lê Văn C", "Hải Phòng")
]

source_df = spark.createDataFrame(source_data, ["customer_id", "name", "city"])

# --------------------------------------------
# SCD TYPE 1 - OVERWRITE (NO HISTORY)
# --------------------------------------------

scd1_df = source_df.alias("src").join(
    target_df.alias("tgt"), on="customer_id", how="left"
).select(
    col("src.customer_id"),
    col("src.name"),
    col("src.city"),
    current_date().alias("start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("is_current")
)

scd1_df.show()
print("=== SCD TYPE 1 ===")

# --------------------------------------------
# SCD TYPE 2 - FULL HISTORY WITH VERSIONING
# --------------------------------------------

new_records = source_df.join(target_df, "customer_id", "left") \
    .filter((target_df["city"].isNull()) | (source_df["city"] != target_df["city"])) \
    .select(
        source_df["customer_id"],
        source_df["name"],
        source_df["city"],
        current_date().alias("start_date"),
        lit(None).cast("date").alias("end_date"),
        lit(True).alias("is_current")
    )

expired_records = target_df.join(source_df, "customer_id", "inner") \
    .filter(target_df["city"] != source_df["city"]) \
    .select(
        target_df["customer_id"],
        target_df["name"],
        target_df["city"],
        target_df["start_date"],
        current_date().alias("end_date"),
        lit(False).alias("is_current")
    )

unchanged_records = target_df.join(source_df, "customer_id", "left_anti")

final_scd2_df = unchanged_records.unionByName(expired_records).unionByName(new_records)

final_scd2_df.show()
print("=== SCD TYPE 2 ===")

# --------------------------------------------
# SCD TYPE 4 - OVERWRITE + HISTORY TABLE
# --------------------------------------------

# History table with archive
history_df = target_df.join(source_df, "customer_id", "inner") \
    .filter(target_df["city"] != source_df["city"]) \
    .select(
        target_df["customer_id"],
        target_df["name"],
        target_df["city"],
        current_timestamp().alias("archived_at")
    )

# Main table gets overwritten like SCD1
main_df_scd4 = scd1_df
history_df.show()
print("=== SCD TYPE 4 - HISTORY TABLE ===")

main_df_scd4.show()
print("=== SCD TYPE 4 - MAIN TABLE ===")
 