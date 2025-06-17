# Gunakan base image resmi Airflow
FROM apache/airflow:2.9.0-python3.10

# Gunakan root untuk install Java dan tools dasar
USER root

# Install Java dan dependensi dasar
RUN apt-get update && apt-get install -y \
    # Dependensi untuk iqplus
    chromium \
    libu2f-udev \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    unzip \
    # Dependensi untuk yfinance/pyspark
    openjdk-17-jdk \
    gcc \
    python3-dev \
    curl \
    wget \
    # --- TAMBAHAN UNTUK IDX EXTRACTOR ---
    firefox-esr \
    # ------------------------------------
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver versi 136.0.7103.92
RUN wget -O /tmp/chromedriver.zip https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/136.0.7103.92/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /tmp/chromedriver && \
    mv /tmp/chromedriver/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver /tmp/chromedriver.zip

# Install Geckodriver (untuk Firefox)
RUN wget -O /tmp/geckodriver.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.36.0/geckodriver-v0.36.0-linux64.tar.gz && \
    tar -xzf /tmp/geckodriver.tar.gz -C /usr/local/bin && \
    chmod +x /usr/local/bin/geckodriver && \
    rm /tmp/geckodriver.tar.gz

# Set environment variable Java untuk PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
# Set path chromium dan chromedriver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver

# Buat direktori untuk downloads dengan permission yang benar
RUN mkdir -p /opt/airflow/downloads && \
    chown -R airflow:root /opt/airflow/downloads && \
    chmod -R 755 /opt/airflow/downloads

# Kembali ke user airflow
USER airflow

# Install dependencies Python
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    yfinance \
    pandas \
    numpy \
    pymongo \
    apache-airflow-providers-mongo==4.0.0 \
    findspark \
    flask \
    flask-cors \
    waitress \
    requests \
    pyspark==3.4.0 \
    selenium \
    webdriver_manager \
    transformers \
    torch==2.1.2 --extra-index-url https://download.pytorch.org/whl/cpu \
    sentencepiece