from pyspark.sql import SparkSession

def main():
   spark = (
       SparkSession.builder
           .appName("IcebergTimeTravelExample")
           .getOrCreate()
   )
   
   catalog_table = "hive_catalog.default.users"
   
   # Liệt kê các snapshot hiện có
   snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at, summary FROM {catalog_table}.snapshots")
   snapshots_df.orderBy("committed_at").show(truncate=False)
   print("=== Danh sách snapshots ===")
   
   # Giả sử chúng ta lấy snapshot đầu tiên (cũ nhất) để time-travel:
   first_snapshot_id = snapshots_df.orderBy("committed_at").first()["snapshot_id"]
   print(f">>> Sẽ time-travel về snapshot_id = {first_snapshot_id}")
   
   # Đọc dữ liệu tại snapshot đó (version-as-of)
   df_time_travel = spark.read \
       .format("iceberg") \
       .option("snapshot-id", first_snapshot_id) \
       .load(catalog_table)
   df_time_travel.show(truncate=False)
   print(f"=== Dữ liệu tại snapshot {first_snapshot_id} ===")
   
   # Hoặc dùng SQL cú pháp VERSION AS OF
   df_sql = spark.sql(f"SELECT * FROM {catalog_table} VERSION AS OF {first_snapshot_id}")
   df_sql.show(truncate=False)
   print(f"=== (SQL) Dữ liệu tại snapshot {first_snapshot_id} ===")
   
   # Time-travel theo timestamp (ví dụ 5 phút trước)
   import datetime, pytz
   ts = (datetime.datetime.now(pytz.UTC) - datetime.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
   print(f">>> Sẽ time-travel theo timestamp = {ts}")
   df_ts = spark.read \
       .format("iceberg") \
       .option("timestamp-as-of", ts) \
       .load(catalog_table)
   df_ts.show(truncate=False)
   print(f"=== Dữ liệu tại timestamp {ts} ===")
   
   spark.stop()

if __name__ == "__main__":
   main()