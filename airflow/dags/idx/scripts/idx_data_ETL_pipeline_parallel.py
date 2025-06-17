from __future__ import annotations
import pendulum
import logging
import os
import sys
import importlib.util

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
import shutil

# Setup logging
logger = logging.getLogger(__name__)

# --- KONFIGURASI DAG UMUM ---
YEARS_TO_SCRAPE = ["2021"] # Tahun yang akan diekstrak
EXTRACTION_POOL_NAME = "idx_etl_pool" # Nama pool untuk semua task ETL
BASE_DOWNLOAD_DIR = "/opt/airflow/data"  # Base directory untuk semua download
DEFAULT_PERIODS = "tw1" # Default periods sesuai dengan script utama

# Karena Anda ingin menyamakan pool untuk semua tahap ETL
TRANSFORM_LOAD_POOL_NAME = EXTRACTION_POOL_NAME
MONGODB_READ_POOL_NAME = EXTRACTION_POOL_NAME


# --- Fungsi-fungsi dari DAG Ekstraksi ---
def validate_parameters(**context):
    """
    Validates input parameters before initiating the extraction process.
    Ensures that 'periods' and 'years' are in valid formats and ranges.
    """
    try:
        periods = DEFAULT_PERIODS
        years = YEARS_TO_SCRAPE

        valid_periods = ['tw1', 'tw2']
        input_periods = [p.strip().lower() for p in periods.split(',')]

        for period in input_periods:
            if period not in valid_periods:
                raise ValueError(f"Invalid period: {period}. Valid periods: {valid_periods}")

        for year in years:
            try:
                year_int = int(year)
                if year_int < 2020 or year_int > 2030:  # Define a reasonable range for years
                    raise ValueError(f"Year {year} is out of reasonable range (2020-2030)")
            except ValueError:
                raise ValueError(f"Invalid year format: {year}")

        logger.info(f"✅ Parameters validated successfully")
        logger.info(f"Years to process: {years}")
        logger.info(f"Periods to process: {input_periods}")

        return {
            'status': 'validated',
            'years': years,
            'periods': periods,
            'validated_periods': input_periods
        }

    except Exception as e:
        logger.error(f"❌ Parameter validation failed: {e}")
        raise AirflowException(f"Parameter validation failed: {e}")


def setup_environment(**context):
    """
    Sets up necessary directories for data downloads.
    Creates a base directory and verifies write permissions.
    """
    try:
        os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)
        logger.info(f"✅ Base download directory ready: {BASE_DOWNLOAD_DIR}")

        test_file = os.path.join(BASE_DOWNLOAD_DIR, "test_airflow_write.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info("✅ Write permission verified for base directory")

        return {
            'status': 'environment_ready',
            'base_dir': BASE_DOWNLOAD_DIR,
        }

    except Exception as e:
        logger.error(f"❌ Environment setup failed: {e}")
        raise AirflowException(f"Environment setup failed: {e}")


def import_and_run_extraction(**context):
    """
    Imports and executes the main extraction function from an external script.
    Includes robust import logic and passes necessary parameters like year, periods, and MongoDB URI.
    """
    year = context.get('year')
    periods = DEFAULT_PERIODS

    logger.info(f"🚀 Starting extraction for year {year} with periods: {periods}")

    try:
        # Menambahkan dags/idx/scripts ke sys.path untuk impor
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')
        
        if scripts_path not in sys.path:
            sys.path.insert(0, scripts_path)
            logger.info(f"Added '{scripts_path}' to sys.path for extraction script import.")

        # Import main_extraction_task dari modul idx_extraction_script
        # Asumsikan idx_extraction_script.py ada di dalam folder 'idx/scripts'
        from idx.scripts.idx_extraction_script import main_extraction_task
        logger.info(f"✅ Successfully imported 'main_extraction_task'.")
        
        # Ambil MONGO_URI dari environment variable
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb-external:27017/")
        
        result = main_extraction_task(
            year=year,
            periods=periods,
            mongo_uri=mongo_uri 
        )
        
        logger.info(f"✅ Extraction completed for year {year}")
        return result
        
    except ImportError as e:
        error_msg = f"❌ Import error for extraction script for year {year}: {e}. Make sure 'idx_extraction_script.py' is in 'dags/idx/scripts'."
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except Exception as e:
        error_msg = f"❌ Extraction failed for year {year}: {e}"
        logger.error(error_msg, exc_info=True)
        raise AirflowException(error_msg)


def cleanup_year_directories(**context):
    """
    Cleans up specific year directories within BASE_DOWNLOAD_DIR for the years being scraped.
    """
    logger.info("🗑️ Starting cleanup of specific year data directories...")
    cleanup_count = 0

    for year in YEARS_TO_SCRAPE:
        year_dir_path = os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}")

        if os.path.exists(year_dir_path) and os.path.isdir(year_dir_path):
            try:
                shutil.rmtree(year_dir_path)
                cleanup_count += 1
                logger.info(f"🗑️ Removed old year directory: {year_dir_path}")
            except Exception as e:
                logger.warning(f"Failed to remove directory {year_dir_path}: {e}")
        else:
            logger.info(f"Directory not found or not a directory, skipping: {year_dir_path}")

    for item in os.listdir(BASE_DOWNLOAD_DIR):
        item_path = os.path.join(BASE_DOWNLOAD_DIR, item)
        if os.path.isfile(item_path) and item.lower().endswith((".zip", ".part", ".tmp", ".png", ".log")):
            try:
                os.remove(item_path)
                cleanup_count += 1
            except Exception as e:
                logger.warning(f"Failed to remove loose file {item_path}: {e}")

    logger.info(f"✅ Cleanup completed. Removed {cleanup_count} items (year directories/loose files).")
    return {'status': 'cleanup_completed', 'items_removed': cleanup_count}


def generate_extraction_summary(**context):
    """
    Generates a summary of the extraction process.
    Provides basic information about the DAG run and processed years.
    """
    try:
        dag_run = context['dag_run']

        summary = {
            'dag_run_id': dag_run.run_id,
            'execution_date': dag_run.execution_date.isoformat(),
            'years_processed': YEARS_TO_SCRAPE,
            'total_years': len(YEARS_TO_SCRAPE),
            'status': 'completed',
            'base_download_dir': BASE_DOWNLOAD_DIR
        }

        logger.info("📊 EXTRACTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"DAG Run ID: {summary['dag_run_id']}")
        logger.info(f"Execution Date: {summary['execution_date']}")
        logger.info(f"Years Processed: {summary['years_processed']}")
        logger.info(f"Total Years: {summary['total_years']}")
        logger.info(f"Download Directory: {summary['base_download_dir']}")
        logger.info("=" * 50)

        return summary

    except Exception as e:
        logger.error(f"❌ Failed to generate summary: {e}")
        return {'status': 'summary_failed', 'error': str(e)}


# --- Fungsi-fungsi dari Skrip Transformasi/Load ---
# Perbaikan Impor: Mengimpor fungsi langsung dari skrip transformasi
try:
    # Menambahkan dags/idx/scripts ke sys.path untuk impor
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')
    
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
        logger.info(f"Added '{scripts_path}' to sys.path for transformation script import.")

    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')
        if scripts_path not in sys.path:
            sys.path.insert(0, scripts_path)
            logger.info(f"Added '{scripts_path}' to sys.path for script imports.")

        from idx.scripts.idx_transformation_load_script import (
            get_source_collections,
            process_and_load_single_collection,
            create_spark_session
        )
        logger.info("✅ Successfully imported all required script functions.")
    except ImportError as e:
        logger.error(f"❌ Failed to import script functions: {e}")
        raise AirflowException(f"Failed to import script functions: {e}")

except ImportError as e:
    logger.error(f"❌ Gagal mengimpor fungsi skrip transformasi: {e}. Pastikan 'idx_transformation_script.py' ada di 'dags/idx/scripts'.")
    raise AirflowException(f"Gagal mengimpor fungsi skrip transformasi: {e}")
except Exception as e:
    logger.error(f"❌ Error tak terduga saat impor skrip transformasi: {e}")
    raise AirflowException(f"Error tak terduga saat impor skrip transformasi: {e}")



# === DAG DEFINITION (Gabungan) ===
with DAG(
        dag_id="idx_data_ETL_pipeline_parallel",
        default_args={
            "owner": "airflow",
            "retry_delay": pendulum.duration(minutes=10),
            "email_on_failure": False,
            "email_on_retry": False,
            "depends_on_past": False,
        },
        description="IDX Financial Data Extraction, Transformation, and Load - Parallel processing",
        schedule="0 0 1 */3 *",
        start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
        catchup=False,
        tags=["idx", "ETL", "extraction", "transform", "load", "parallel", "financial-data"],
        max_active_runs=1,
        dagrun_timeout=pendulum.duration(hours=18),
) as dag:
    # --- Stage 1: Data Extraction ---
    validate_params = PythonOperator(
        task_id="validate_parameters",
        python_callable=validate_parameters,
        provide_context=True,
    )

    setup_env = PythonOperator(
        task_id="setup_environment",
        python_callable=setup_environment,
        provide_context=True,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_year_data_folders",
        python_callable=cleanup_year_directories,
        provide_context=True,
    )

    check_resources = BashOperator(
        task_id="check_system_resources",
        bash_command="""
        echo "=== SYSTEM RESOURCES CHECK ==="
        echo "Memory Usage:"
        free -h
        echo ""
        echo "Disk Usage:"
        df -h /opt/airflow/data || df -h /tmp
        echo ""
        echo "CPU Info (number of processors):"
        nproc
        echo ""
        echo "=== CHECK COMPLETED ==="
        """,
    )

    start_extractions = EmptyOperator(task_id="start_all_extractions")

    # Extraction tasks group (parallel by year)
    with TaskGroup("extraction_by_year") as extraction_group:
        extraction_tasks = []

        for year in YEARS_TO_SCRAPE:
            extraction_task = PythonOperator(
                task_id=f"extract_year_{year}",
                python_callable=import_and_run_extraction,
                op_kwargs={
                    "year": year,
                    "periods": DEFAULT_PERIODS
                },
                pool=EXTRACTION_POOL_NAME,
                pool_slots=1,
                execution_timeout=pendulum.duration(hours=6),
                retry_delay=pendulum.duration(minutes=15),
                provide_context=True,
            )
            extraction_tasks.append(extraction_task)

    end_extractions = EmptyOperator(task_id="end_all_extractions")

    generate_extraction_summary_task = PythonOperator(
        task_id="generate_extraction_summary",
        python_callable=generate_extraction_summary,
        provide_context=True,
        trigger_rule="all_done",
    )

    # --- Stage 2: Data Transformation and Loading ---
    start_transform_load = EmptyOperator(task_id="start_transform_load_process")

    wait_for_extraction_completion = EmptyOperator(task_id="wait_for_extraction_completion")

    check_spark_connection = PythonOperator(
        task_id="check_spark_session_pre_transform",
        python_callable=lambda: create_spark_session().stop(),
        execution_timeout=pendulum.duration(minutes=5),
        pool=TRANSFORM_LOAD_POOL_NAME,
        pool_slots=1,
    )

    get_collections_list = PythonOperator(
        task_id="get_source_collections_from_mongodb",
        python_callable=get_source_collections,
        provide_context=True,
        pool=MONGODB_READ_POOL_NAME,
        pool_slots=1,
    )

    # --- PERBAIKAN INDENTASI DI SINI ---
    # Task Group untuk proses transformasi dan pemuatan paralel
    with TaskGroup("parallel_transform_load_collections") as transform_load_group:
        # Menggunakan Dynamic Task Mapping
        # get_source_collections() harus mengembalikan LIST OF STRINGS (misal: ['reports_2021_tw1', ...])
        # agar process_and_load_single_collection(source_collection_name: str) menerima 1 argumen
        process_collection_tasks = PythonOperator.partial(
            task_id="process_single_collection",
            python_callable=process_and_load_single_collection,
            pool=TRANSFORM_LOAD_POOL_NAME,
            execution_timeout=pendulum.duration(minutes=60),
            retry_delay=pendulum.duration(minutes=10),
        ).expand(
            op_args=[get_collections_list.output]
        )

    end_transform_load = EmptyOperator(task_id="end_transform_load_process")

    # --- Task Dependencies ---
    validate_params >> setup_env >> cleanup_task >> check_resources
    check_resources >> start_extractions >> extraction_group >> end_extractions
    end_extractions >> generate_extraction_summary_task

    generate_extraction_summary_task >> wait_for_extraction_completion
    wait_for_extraction_completion >> start_transform_load

    start_transform_load >> check_spark_connection >> get_collections_list
    get_collections_list >> transform_load_group >> end_transform_load