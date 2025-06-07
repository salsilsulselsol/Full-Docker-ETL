from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

# --- DAFTAR TAHUN UNTUK DIPROSES ---
# Bisa juga diambil dari Airflow Variables atau Params
YEARS_TO_SCRAPE = ["2021", "2022", "2023", "2024"]

# Nama pool yang akan digunakan. PASTIKAN ANDA MEMBUAT POOL INI DI AIRFLOW UI.
# Pergi ke Admin -> Pools -> Create. Beri nama 'idx_extraction_pool' dan atur Slots (misal: 2)
EXTRACTION_POOL_NAME = "idx_extraction_pool"

# Impor fungsi dari skrip baru
from idx.scripts.idx_extraction_script import main_extraction_task

with DAG(
    dag_id="idx_data_extraction_parallel_by_year",
    default_args={"owner": "airflow"},
    description="Mengekstrak data finansial IDX secara paralel per tahun menggunakan pool.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["idx", "extraction", "parallel", "python"],
    params={
        "periods": Param("tw1,tw2,tw3,audit", type="string", title="Periode Laporan"),
    },
) as dag:
    start_all_extractions = EmptyOperator(task_id="start_all_extractions")

    end_all_extractions = EmptyOperator(task_id="end_all_extractions")

    # Loop melalui setiap tahun untuk membuat task PythonOperator secara dinamis
    for year in YEARS_TO_SCRAPE:
        extraction_task = PythonOperator(
            task_id=f"run_extraction_for_year_{year}",
            python_callable=main_extraction_task,
            op_kwargs={
                "year": year,
                "periods": "{{ params.periods }}" # Mengambil periode dari parameter DAG
            },
            pool=EXTRACTION_POOL_NAME, # Menetapkan task ke pool yang sudah dibuat
        )
        
        start_all_extractions >> extraction_task >> end_all_extractions