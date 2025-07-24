#Sử dụng image python làm base
FROM python:3.9-slim

#Thiết lập thư mục làm việc trong container
WORKDIR /app

#Sao chép file mã nguồn vào container
COPY hello.py .

#Lệnh mặc định khi container khởi chạy
CMD ["python", "hello.py"]