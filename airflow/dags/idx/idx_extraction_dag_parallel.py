from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

# Nama network Docker Compose yang eksplisit dari docker-compose.yml
DOCKER_NETWORK_NAME = "project_etl_network" # Harus cocok dengan nama network di docker-compose.yml

# Nama image yang dibangun oleh docker-compose.yml untuk ekstraksi
EXTRACTOR_IMAGE_NAME = "idx_etl_extractor_custom:latest"

# --- DAFTAR TAHUN DAN PERIODE UNTUK DIPROSES ---
YEARS_TO_SCRAPE = ["2021", "2022"]#, "2023", "2024", "2025"] # Sesuaikan dengan kebutuhan Anda
ALL_PERIODS_STRING = "tw1,tw2,tw3,audit" # Semua periode yang diinginkan
# ---------------------------------------------

# Variabel default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1, 
    "retry_delay": pendulum.duration(minutes=2),
    # "pool": "docker_tasks_pool", # Pertimbangkan menggunakan pool untuk membatasi konkurensi DockerOperator
}

with DAG(
    dag_id="idx_data_extraction_parallel_by_year", # ID DAG baru agar tidak bentrok dengan yang lama
    default_args=default_args,
    description="Extracts IDX financial data in parallel for each year",
    schedule=None, 
    start_date=pendulum.datetime(2024, 5, 26, tz="Asia/Jakarta"), 
    catchup=False, 
    tags=["idx", "extraction", "finance", "docker", "parallel"],
) as dag:
    start_all_extractions = EmptyOperator(task_id="start_all_extraction_processes")

    # List untuk menampung semua task ekstraksi yang dibuat secara dinamis
    extraction_tasks = []

    # Loop melalui setiap tahun untuk membuat task DockerOperator
    for year_to_process in YEARS_TO_SCRAPE:
        task_id_for_year = f"run_idx_extraction_script_for_year_{year_to_process}"
        
        # Variabel lingkungan spesifik untuk task ini
        current_extraction_env_vars = {
            "MONGO_URI": "mongodb://mongo_db:27017/", 
            "SCRAPE_YEARS": year_to_process, # Hanya tahun ini yang diproses oleh task ini
            "SCRAPE_PERIODS": ALL_PERIODS_STRING, # Skrip akan memproses semua periode untuk tahun ini
            "PYTHONUNBUFFERED": "1", 
        }

        extraction_task_for_year = DockerOperator(
            task_id=task_id_for_year,
            image=EXTRACTOR_IMAGE_NAME, 
            api_version="auto", 
            auto_remove="success", 
            docker_url="unix://var/run/docker.sock", 
            network_mode=DOCKER_NETWORK_NAME, 
            environment=current_extraction_env_vars, 
            force_pull=False, 
            tty=True, 
            mount_tmp_dir=False, 
            # pool="docker_tasks_pool", # Jika Anda menggunakan pool
        )
        extraction_tasks.append(extraction_task_for_year)

    end_all_extractions = EmptyOperator(task_id="end_all_extraction_processes")

    # Mendefinisikan urutan: semua task ekstraksi berjalan setelah 'start' dan sebelum 'end'
    # Semua task di dalam extraction_tasks akan berjalan secara paralel (dibatasi oleh konfigurasi Airflow)
    start_all_extractions >> extraction_tasks >> end_all_extractions