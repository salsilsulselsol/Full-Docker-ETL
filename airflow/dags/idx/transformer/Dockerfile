# airflow/dags/idx/transformer/Dockerfile
# Gunakan image Spark resmi yang menyertakan Python (PySpark) dan Java.
FROM bitnami/spark:3.5.1-debian-12-r0

# Setel variabel lingkungan
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/bitnami/spark
ENV PYSPARK_PYTHON=/usr/bin/python3

# Ganti ke user root untuk menginstal paket.
USER root

# Instal pip jika belum ada atau untuk upgrade.
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setel direktori kerja
WORKDIR /app

# Install Python dependencies langsung menggunakan pip3
RUN pip3 install --no-cache-dir \
    pymongo

# Salin skrip transformasi
COPY transformation_docker.py .

# Ganti kembali ke user non-root (1001 untuk image bitnami/spark)
USER 1001

# Perintah untuk menjalankan skrip PySpark menggunakan spark-submit.
CMD ["spark-submit", \
     "--deploy-mode", "client", \
     "transformation_docker.py"]