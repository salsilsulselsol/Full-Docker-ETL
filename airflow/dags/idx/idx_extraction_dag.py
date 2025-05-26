from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

# Nama network Docker Compose yang eksplisit dari docker-compose.yml
DOCKER_NETWORK_NAME = "project_etl_network" # Harus cocok dengan nama network di docker-compose.yml

# Nama image yang dibangun oleh docker-compose.yml untuk ekstraksi
EXTRACTOR_IMAGE_NAME = "idx_etl_extractor_custom:latest"

# Variabel default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1, 
    "retry_delay": pendulum.duration(minutes=2),
}

with DAG(
    dag_id="idx_data_extraction", 
    default_args=default_args,
    description="Extracts financial data from IDX",
    schedule=None, 
    start_date=pendulum.datetime(2024, 5, 26, tz="Asia/Jakarta"), 
    catchup=False, 
    tags=["idx", "extraction", "finance", "docker"],
) as dag:
    start_extraction = EmptyOperator(task_id="start_extraction_process")

    # --- PERUBAHAN VARIABEL LINGKUNGAN ---
    extraction_env_vars = {
        "MONGO_URI": "mongodb://mongo_db:27017/", 
        "SCRAPE_YEARS": "2021,2022,2023,2024,2025", # Semua tahun yang tersedia (sesuaikan jika perlu)
        "SCRAPE_PERIODS": "tw1,tw2,tw3,audit",     # Semua periode, pastikan huruf kecil
        "PYTHONUNBUFFERED": "1", 
    }
    # ------------------------------------

    run_idx_extraction_script = DockerOperator(
        task_id="run_idx_extraction_script",
        image=EXTRACTOR_IMAGE_NAME, 
        api_version="auto", 
        auto_remove="success", 
        docker_url="unix://var/run/docker.sock", 
        network_mode=DOCKER_NETWORK_NAME, 
        environment=extraction_env_vars, 
        force_pull=False, 
        tty=True, 
        mount_tmp_dir=False, 
    )

    end_extraction = EmptyOperator(task_id="end_extraction_process")

    start_extraction >> run_idx_extraction_script >> end_extraction