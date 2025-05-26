from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState # <-- TAMBAHKAN IMPOR INI

# Nama network Docker Compose yang eksplisit dari docker-compose.yml
DOCKER_NETWORK_NAME = "project_etl_network"

# Nama image yang dibangun oleh docker-compose.yml untuk transformasi
TRANSFORMER_IMAGE_NAME = "idx_etl_transformer_custom:latest"

# Variabel default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    # "pool": "docker_tasks_pool", # Opsional
}

with DAG(
    dag_id="idx_data_transform_load", # ID DAG spesifik untuk transformasi & load IDX
    default_args=default_args,
    description="Transforms and loads IDX financial data from staging to production",
    schedule=None, # Bisa diatur untuk berjalan setelah DAG ekstraksi, atau dipicu
    start_date=pendulum.datetime(2024, 5, 26, tz="Asia/Jakarta"),
    catchup=False,
    tags=["idx", "transform", "load", "finance", "docker"],
) as dag:
    start_transform_load = EmptyOperator(task_id="start_transform_load_process")

    # Sensor untuk menunggu DAG ekstraksi IDX selesai
    wait_for_extraction = ExternalTaskSensor(
        task_id="wait_for_idx_extraction_completion",
        external_dag_id="idx_data_extraction", # ID dari DAG ekstraksi yang ditunggu
        allowed_states=[DagRunState.SUCCESS], # Menggunakan Enum DagRunState
        failed_states=[DagRunState.FAILED],   # Menggunakan Enum DagRunState
        # Anda juga bisa menambahkan skipped_states jika perlu:
        # skipped_states=[DagRunState.SKIPPED],
        mode="poke", # Mode 'poke' akan memeriksa secara berkala
        poke_interval=60, # Cek setiap 60 detik
        timeout=7200, # Timeout setelah 2 jam menunggu (sesuaikan)
    )

    # Variabel lingkungan untuk kontainer transformasi
    transformation_env_vars = {
        "MONGO_URI": "mongodb://mongo_db:27017/", # Menggunakan nama service MongoDB
        "SPARK_MASTER_URL": "local[*]",
        "SPARK_EXECUTOR_MEMORY": "2g",
        "SPARK_DRIVER_MEMORY": "2g",
        "PYTHONUNBUFFERED": "1",
    }

    # Task untuk menjalankan skrip transformasi dan load data IDX
    run_idx_transform_load_script = DockerOperator(
        task_id="run_idx_transform_load_script",
        image=TRANSFORMER_IMAGE_NAME,
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK_NAME,
        environment=transformation_env_vars,
        force_pull=False,
        tty=True,
        mount_tmp_dir=False,
        # pool="docker_tasks_pool",
    )

    end_transform_load = EmptyOperator(task_id="end_transform_load_process")

    # Urutan task: tunggu ekstraksi (jika sensor digunakan), lalu transform/load
    start_transform_load >> wait_for_extraction >> run_idx_transform_load_script >> end_transform_load