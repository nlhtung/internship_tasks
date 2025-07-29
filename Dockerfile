FROM bitnami/spark:3.5.1
USER root
# Nhúng tất cả JAR Iceberg vào Spark jars
RUN mkdir -p /opt/bitnami/spark/jars/
COPY libs/*.jar /opt/bitnami/spark/jars/
# Copy cấu hình và code
COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY conf/hive-site.xml /opt/bitnami/spark/conf/hive-site.xml
COPY app/ /opt/app/
RUN mkdir -p /opt/iceberg/warehouse \
    && chown -R 1001:1001 /opt/iceberg/warehouse
USER 1001
WORKDIR /opt/app
# Không định nghĩa ENTRYPOINT: chúng ta chỉ build image, chạy Spark master/worker, rồi submit thủ công