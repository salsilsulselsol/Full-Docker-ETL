# Dockerfile (di project-root/idx-etl-airflow/)
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Tidak perlu USER root di sini jika hanya menginstal paket pip untuk user airflow

# Langsung ganti ke user airflow SEBELUM menginstal paket Python
USER airflow

# Instal provider atau library Python tambahan yang dibutuhkan oleh DAGs Anda.
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker

# USER airflow # Tidak perlu lagi karena sudah di atas