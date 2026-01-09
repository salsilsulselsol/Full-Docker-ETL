# Full Docker ETL - Big Data Pipeline

![Docker](https://img.shields.io/badge/Docker-Container-blue) ![Apache Airflow](https://img.shields.io/badge/Airflow-Orchestration-TEAL) ![Python Flask](https://img.shields.io/badge/Flask-API-black) ![MongoDB](https://img.shields.io/badge/MongoDB-Database-green)

Repository ini berfungsi sebagai **Backend** dari sistem Analitik Saham Big Data. Sistem ini bertanggung jawab untuk mengambil (Extract), membersihkan (Transform), dan menyimpan (Load) data pasar modal secara otomatis dan terjadwal.

Frontend untuk memvisualisasikan data ini dapat ditemukan di:
🔗 **[salsilsulselsol/dashboard_bigdata](https://github.com/salsilsulselsol/dashboard_bigdata)**

## Arsitektur Sistem

Proyek ini berjalan sepenuhnya di atas ekosistem **Docker** dengan layanan-layanan berikut yang saling terorkestrasi:

1.  **Apache Airflow (Celery Executor):** Mengatur jadwal dan alur kerja pipeline ETL.
    * *Worker & Scheduler:* Menjalankan task scraping dan pemrosesan data.
    * *Redis:* Message broker untuk komunikasi antar worker Airflow.
    * *PostgreSQL:* Metadata database untuk Airflow.
2.  **Data Processing:** Script Python (dengan dukungan Pandas/Spark) untuk mengolah data mentah dari:
    * **IDX (Bursa Efek Indonesia):** Laporan keuangan tahunan & kuartalan.
    * **YFinance:** Data historis harga saham.
    * **IQPlus:** Berita emiten terkini.
3.  **Storage (Data Warehouse):**
    * **MongoDB:** Menyimpan data semi-terstruktur hasil olahan (`Yfinance_Load`, `Iqplus`, `idx_financial_data`).
4.  **Serving Layer:**
    * **Flask API:** Menyediakan endpoint RESTful agar data di MongoDB dapat diakses oleh Frontend Laravel.

## Tech Stack

* **Infrastructure:** Docker, Docker Compose
* **Orchestration:** Apache Airflow 2.x
* **Database:** MongoDB (Data), PostgreSQL (Metadata), Redis (Broker/Cache)
* **API Framework:** Python Flask
* **ETL Logic:** Python, Pandas, BeautifulSoup (Scraping)

## Alur Pipeline (DAGs)

Sistem ini memiliki beberapa DAG (*Directed Acyclic Graph*) utama:

* `idx_data_ETL_pipeline_sequential`: Pipeline sekusensial untuk mengunduh dan memproses laporan keuangan IDX (Tahun 2020-2025, Kuartal 1-Audit).
* `extract_iqplus_news_dag`: Mengambil berita terbaru dari portal IQPlus.
* `TransForm_Load_Yfinance`: Mengambil dan normalisasi data harga saham harian.

## API Endpoints (Flask)

Layanan Flask berjalan di port `5000` dan mengekspos endpoint berikut:

### 📈 Emiten & Saham
* `GET /api/companies`: List semua kode perusahaan yang tersedia.
* `GET /api/data/<company_name>`: Data historis saham dengan filter (harian/bulanan/tahunan).
* `GET /api/agg_types/<company_name>`: Tipe agregasi yang tersedia.

### 📰 Berita (IQPlus)
* `GET /api/iqplus/news`: Mengambil semua berita (bisa search by title).
* `GET /api/iqplus/news/<news_id>`: Detail berita berdasarkan ID.

### 📊 Laporan Keuangan (IDX)
* `GET /api/reports/list/<year>/<period>`: Daftar laporan keuangan per tahun/periode.
* `GET /api/reports/detail/<company_code>/<year>/<period>`: Detail isi laporan keuangan spesifik.

## Cara Menjalankan

1.  **Clone Repository**
    ```bash
    git clone [https://github.com/salsilsulselsol/Full-Docker-ETL.git](https://github.com/salsilsulselsol/Full-Docker-ETL.git)
    cd Full-Docker-ETL
    ```

2.  **Setup Environment**
    Pastikan Docker Desktop / Docker Engine sudah berjalan.

3.  **Jalankan Container**
    ```bash
    docker-compose up -d --build
    ```
    *Proses ini akan memakan waktu cukup lama saat pertama kali untuk membuild image Airflow dan mengunduh dependencies.*

4.  **Akses Layanan**
    * **Airflow Webserver:** `http://localhost:8080` (Login default: `admin` / `admin`)
    * **Flask API:** `http://localhost:5000`
    * **MongoDB:** `localhost:27017`

## Tim Pengembang

Proyek ini dikerjakan sebagai bagian *backend engineering* untuk tugas akhir mata kuliah Big Data.
* **Frontend & Dashboard:** [Lihat Repo Dashboard](https://github.com/sif4imnurul/dashboard_bigdata)

---
*Catatan: Pastikan container ini berjalan bersamaan dengan repository frontend agar data dapat tampil di dashboard.*
