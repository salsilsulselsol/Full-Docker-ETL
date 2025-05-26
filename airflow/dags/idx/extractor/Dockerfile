# airflow/dags/idx/extractor/Dockerfile
# Base image with Python
FROM python:3.9-slim

# Set environment variables for non-interactive installs
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install system dependencies including Firefox ESR and Geckodriver
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt-get update && apt-get install -y --no-install-recommends \
    firefox-esr \
    wget \
    ca-certificates \
    libxt6 \
    libxrender1 \
    libxrandr2 \
    libdbus-glib-1-2 \
    libgtk-3-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- PERUBAHAN DI SINI ---
ARG GECKODRIVER_VERSION=v0.36.0
# -------------------------
RUN wget https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VERSION}/geckodriver-${GECKODRIVER_VERSION}-linux64.tar.gz -O /tmp/geckodriver.tar.gz \
    && tar -C /usr/local/bin -xzf /tmp/geckodriver.tar.gz \
    && rm /tmp/geckodriver.tar.gz \
    && chmod +x /usr/local/bin/geckodriver

WORKDIR /app

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    pandas \
    requests \
    "selenium>=4.0.0" \
    pymongo \
    lxml

# Salin skrip ekstraksi
COPY extraction_docker.py .

# Buat direktori unduhan dan atur izin
RUN mkdir -p /app/downloads && chmod -R 777 /app/downloads

# Perintah untuk menjalankan skrip, menggunakan Tini sebagai entrypoint
ENTRYPOINT ["/tini", "--"]
CMD ["python", "extraction_docker.py"]