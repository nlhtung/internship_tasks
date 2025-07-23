-- BATCH MỚI CẬP NHẬT
CREATE TEMP TABLE stg_customer (
  customer_id INT,
  name TEXT,
  city TEXT
);
INSERT INTO stg_customer VALUES
(1, 'Nguyễn Văn A', 'Hà Nội'),
(2, 'Trần Thị B', 'Đà Nẵng'),
(3, 'Lê Văn C', 'Hải Phòng');
-- BẢNG DIMENSION CHÍNH
DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
  customer_id INT,
  name TEXT,
  city TEXT,
  start_date DATE,
  end_date DATE,
  is_current BOOLEAN
);
INSERT INTO dim_customer VALUES
(1, 'Nguyễn Văn A', 'HCM', '2024-01-01', NULL, TRUE),
(2, 'Trần Thị B', 'Đà Nẵng', '2024-01-01', NULL, TRUE);
-- ========================
-- SCD TYPE 1 - OVERWRITE
-- ========================
-- Ghi đè nếu tồn tại
UPDATE dim_customer tgt
SET name = src.name,
   city = src.city
FROM stg_customer src
WHERE tgt.customer_id = src.customer_id;
-- Thêm mới nếu chưa tồn tại
INSERT INTO dim_customer (customer_id, name, city, start_date, end_date, is_current)
SELECT src.customer_id, src.name, src.city, CURRENT_DATE, NULL, TRUE
FROM stg_customer src
LEFT JOIN dim_customer tgt ON src.customer_id = tgt.customer_id
WHERE tgt.customer_id IS NULL;
-- In kết quả SCD Type 1
SELECT '----- SCD TYPE 1 RESULT -----';
SELECT * FROM dim_customer ORDER BY customer_id;
-- ========================
-- SCD TYPE 2 - DÙNG to_jsonb()
-- ========================
-- Đóng các bản ghi cũ nếu có thay đổi
UPDATE dim_customer tgt
SET end_date = CURRENT_DATE,
   is_current = FALSE
FROM stg_customer src
WHERE tgt.customer_id = src.customer_id
 AND tgt.is_current = TRUE
 AND to_jsonb(tgt.*) IS DISTINCT FROM to_jsonb(src.*);
-- Thêm bản ghi mới nếu có thay đổi hoặc mới hoàn toàn
INSERT INTO dim_customer (customer_id, name, city, start_date, end_date, is_current)
SELECT src.customer_id, src.name, src.city, CURRENT_DATE, NULL, TRUE
FROM stg_customer src
LEFT JOIN dim_customer tgt
 ON src.customer_id = tgt.customer_id AND tgt.is_current = TRUE
WHERE tgt.customer_id IS NULL
  OR to_jsonb(tgt.*) IS DISTINCT FROM to_jsonb(src.*);
-- In kết quả SCD Type 2
SELECT '----- SCD TYPE 2 RESULT -----';
SELECT * FROM dim_customer ORDER BY customer_id, start_date;
-- ========================
-- SCD TYPE 4 - DÙNG to_jsonb()
-- ========================
DROP TABLE IF EXISTS dim_customer_history;
CREATE TABLE dim_customer_history (
  customer_id INT,
  name TEXT,
  city TEXT,
  archived_at TIMESTAMP
);
-- Ghi bản cũ vào bảng lịch sử nếu có thay đổi
INSERT INTO dim_customer_history (customer_id, name, city, archived_at)
SELECT tgt.customer_id, tgt.name, tgt.city, CURRENT_TIMESTAMP
FROM dim_customer tgt
JOIN stg_customer src ON tgt.customer_id = src.customer_id
WHERE to_jsonb(tgt.*) IS DISTINCT FROM to_jsonb(src.*);
-- Ghi đè dữ liệu mới
UPDATE dim_customer tgt
SET name = src.name,
   city = src.city
FROM stg_customer src
WHERE tgt.customer_id = src.customer_id;
-- Thêm mới
INSERT INTO dim_customer (customer_id, name, city, start_date, end_date, is_current)
SELECT src.customer_id, src.name, src.city, CURRENT_DATE, NULL, TRUE
FROM stg_customer src
LEFT JOIN dim_customer tgt ON src.customer_id = tgt.customer_id
WHERE tgt.customer_id IS NULL;
-- In bảng chính
SELECT '----- SCD TYPE 4 - MAIN TABLE -----';
SELECT * FROM dim_customer ORDER BY customer_id;
-- In bảng lịch sử
SELECT '----- SCD TYPE 4 - HISTORY TABLE -----';
SELECT * FROM dim_customer_history ORDER BY archived_at;