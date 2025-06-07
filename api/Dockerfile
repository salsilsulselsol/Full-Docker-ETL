# Gunakan image dasar Python
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install sudo
USER root
RUN apt-get update && apt-get install -y sudo

# Salin semua kode ke image
COPY . /app

# Install requirements menggunakan sudo
RUN sudo pip install --upgrade pip && \
    sudo pip install --no-cache-dir \
        Flask==2.1.0 \
        Flask-Cors==3.0.10 \
        pymongo==4.0.2 \
        waitress==2.1.1 \
        requests==2.26.0 \
        werkzeug==2.0.3

# Buka port 5000
EXPOSE 5000

# Jalankan aplikasi
CMD ["waitress-serve", "--host", "0.0.0.0", "--port", "5000", "app:app"]
