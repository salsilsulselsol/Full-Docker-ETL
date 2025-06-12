from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

# --- DAFTAR TAHUN UNTUK DIPROSES ---
YEARS_TO_SCRAPE = ["2021", "2022", "2023", "2024"]

# Nama pool yang akan digunakan
EXTRACTION_POOL_NAME = "idx_extraction_pool"

def import_and_run_extraction(**context):
    """Import dan jalankan fungsi ekstraksi."""
    try:
        # Import di dalam fungsi untuk menghindari import error saat DAG parsing
        import sys
        import os
        
        # Tambahkan path dags ke sys.path jika belum ada
        dags_path = "/opt/airflow/dags"
        if dags_path not in sys.path:
            sys.path.insert(0, dags_path)
        
        # Import fungsi ekstraksi
        from idx.scripts.idx_extraction_script import main_extraction_task
        
        # Jalankan fungsi ekstraksi
        return main_extraction_task(**context)
        
    except ImportError as e:
        print(f"Import error: {e}")
        # Fallback: coba import langsung
        try:
            from dags.idx.scripts.idx_extraction_script import main_extraction_task
            return main_extraction_task(**context)
        except ImportError as e2:
            print(f"Fallback import error: {e2}")
            raise Exception(f"Tidak dapat mengimport idx_extraction_script: {e}, {e2}")

with DAG(
    dag_id="idx_data_extraction_parallel_by_year",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="Mengekstrak data finansial IDX secara paralel per tahun menggunakan pool.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["idx", "extraction", "parallel", "python"],
    params={
        "periods": Param(
            default="tw1,tw2,tw3,audit", 
            type="string", 
            title="Periode Laporan",
            description="Periode laporan yang akan diekstrak (pisahkan dengan koma)"
        ),
    },
    max_active_runs=1,  # Batasi hanya 1 DAG run aktif
) as dag:

    start_all_extractions = EmptyOperator(task_id="start_all_extractions")
    end_all_extractions = EmptyOperator(task_id="end_all_extractions")

    # Loop melalui setiap tahun untuk membuat task PythonOperator secara dinamis
    extraction_tasks = []
    
    for year in YEARS_TO_SCRAPE:
        extraction_task = PythonOperator(
            task_id=f"run_extraction_for_year_{year}",
            python_callable=import_and_run_extraction,
            op_kwargs={
                "year": year,
                "periods": "{{ params.periods }}"
            },
            pool=EXTRACTION_POOL_NAME,
            pool_slots=1,  # Setiap task menggunakan 1 slot
            execution_timeout=pendulum.duration(hours=4),  # Timeout 4 jam per task
        )
        
        extraction_tasks.append(extraction_task)
        start_all_extractions >> extraction_task >> end_all_extractions