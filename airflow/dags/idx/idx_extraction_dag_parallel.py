from __future__ import annotations
import pendulum
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
import os # Import os for environment variable access

# Setup logging
logger = logging.getLogger(__name__)

# --- KONFIGURASI DAG ---
YEARS_TO_SCRAPE = ["2021", "2022", "2023", "2024"]
EXTRACTION_POOL_NAME = "idx_extraction_pool"
BASE_DOWNLOAD_DIR = "/opt/airflow/data" # Base directory for all downloads

# Default periods sesuai dengan script utama
DEFAULT_PERIODS = "tw1,tw2,tw3,audit"

def validate_parameters(**context):
    """
    Validates input parameters before initiating the extraction process.
    Ensures that 'periods' and 'years' are in valid formats and ranges.
    """
    try:
        periods = context.get('params', {}).get('periods', DEFAULT_PERIODS)
        years = YEARS_TO_SCRAPE 
        
        valid_periods = ['tw1', 'tw2', 'tw3', 'audit']
        input_periods = [p.strip().lower() for p in periods.split(',')]
        
        for period in input_periods:
            if period not in valid_periods:
                raise ValueError(f"Invalid period: {period}. Valid periods: {valid_periods}")
        
        for year in years:
            try:
                year_int = int(year)
                if year_int < 2020 or year_int > 2030: # Define a reasonable range for years
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
        # Raising an AirflowException marks the task as failed in Airflow UI
        raise AirflowException(f"Parameter validation failed: {e}")

def setup_environment(**context):
    """
    Sets up necessary directories for data downloads.
    Creates a base directory and year-specific subdirectories, and verifies write permissions.
    """
    import os
    
    try:
        os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)
        logger.info(f"✅ Base download directory ready: {BASE_DOWNLOAD_DIR}")
        
        test_file = os.path.join(BASE_DOWNLOAD_DIR, "test_airflow_write.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info("✅ Write permission verified for base directory")
        
        for year in YEARS_TO_SCRAPE:
            year_dir = os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}")
            os.makedirs(year_dir, exist_ok=True)
            logger.info(f"✅ Year directory ready: {year_dir}")
        
        return {
            'status': 'environment_ready',
            'base_dir': BASE_DOWNLOAD_DIR,
            'year_dirs': [os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}") for year in YEARS_TO_SCRAPE]
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
    periods = context.get('params', {}).get('periods', DEFAULT_PERIODS) # Ambil dari params jika ada, fallback ke DEFAULT_PERIODS
    
    logger.info(f"🚀 Starting extraction for year {year} with periods: {periods}")
    
    try:
        import sys
        import os
        import importlib.util # For more robust module import

        possible_paths = [
            "/opt/airflow/dags",
            "/opt/airflow/dags/idx/scripts", # Common location if organized in subfolders
            "/opt/airflow/plugins", # Alternative location for custom plugins
            os.path.dirname(os.path.abspath(__file__)) # Directory of the current DAG file
        ]
        
        for path in possible_paths:
            if path not in sys.path:
                sys.path.insert(0, path)
        
        main_extraction_task_callable = None
        import_attempts = [
            "idx.scripts.idx_extraction_script", # Preferred if structured as a package
            "idx_extraction_script",            # If the script is directly in dags/ or plugins/
            "dags.idx.scripts.idx_extraction_script", # For specific Airflow structures
            "scripts.idx_extraction_script"
        ]
        
        for module_path in import_attempts:
            try:
                spec = importlib.util.find_spec(module_path)
                if spec:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    if hasattr(module, 'main_extraction_task'):
                        main_extraction_task_callable = getattr(module, 'main_extraction_task')
                        logger.info(f"✅ Successfully imported 'main_extraction_task' from: {module_path}")
                        break
            except Exception as e:
                logger.debug(f"Failed to import from {module_path}: {e}")
                continue
        
        if main_extraction_task_callable is None:
            raise ImportError("Could not import main_extraction_task from any path. Ensure 'idx_extraction_script.py' is correctly placed and accessible.")
        
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        
        result = main_extraction_task_callable(
            year=year,
            periods=periods, # Pass the periods string to the script
            mongo_uri=mongo_uri # Pass mongo_uri to the script
        )
        
        logger.info(f"✅ Extraction completed for year {year}")
        return result
        
    except ImportError as e:
        error_msg = f"❌ Import error for year {year}: {e}"
        logger.error(error_msg)
        raise AirflowException(error_msg)
        
    except Exception as e:
        error_msg = f"❌ Extraction failed for year {year}: {e}"
        logger.error(error_msg, exc_info=True) # Log full traceback for better debugging
        raise AirflowException(error_msg)

def cleanup_old_data(**context):
    """
    Cleans up old downloaded and extracted data files.
    Removes files older than 7 days and empty company directories within the year folders.
    Cleanup can be toggled via DAG parameters.
    """
    import os
    import shutil
    from datetime import datetime, timedelta
    
    try:
        do_cleanup = context['params'].get('cleanup_old_data', True)
        if not do_cleanup:
            logger.info("ℹ️ Cleanup of old data is disabled by parameter.")
            return {'status': 'skipped', 'message': 'Cleanup disabled'}

        cutoff_time = datetime.now() - timedelta(days=7) # Define cutoff for old files
        cleanup_count = 0
        
        for year in YEARS_TO_SCRAPE:
            year_dir = os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}")
            if os.path.exists(year_dir):
                for item in os.listdir(year_dir):
                    item_path = os.path.join(year_dir, item)
                    try:
                        if os.path.isfile(item_path):
                            file_time = datetime.fromtimestamp(os.path.getmtime(item_path))
                            # Only remove certain temporary file types
                            if file_time < cutoff_time and item.lower().endswith((".zip", ".part", ".tmp", ".png")): # Added .png for screenshots
                                os.remove(item_path)
                                cleanup_count += 1
                                logger.info(f"Removed old file: {item_path}")
                        elif os.path.isdir(item_path):
                            # Remove old, empty company directories. If you want to remove ALL old company directories regardless of empty,
                            # remove `and not any(os.listdir(item_path))` condition, but be careful!
                            dir_time = datetime.fromtimestamp(os.path.getmtime(item_path))
                            if dir_time < cutoff_time and item != '.gitkeep' and not any(os.listdir(item_path)): 
                                shutil.rmtree(item_path)
                                cleanup_count += 1
                                logger.info(f"Removed old empty directory: {item_path}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup {item_path}: {e}")
        
        logger.info(f"✅ Cleanup completed. Removed {cleanup_count} old items.")
        return {'status': 'cleanup_completed', 'items_removed': cleanup_count}
        
    except Exception as e:
        logger.warning(f"⚠️ Cleanup failed: {e}")
        # Do not raise AirflowException here if cleanup is not a critical path failure
        return {'status': 'cleanup_failed', 'error': str(e)}

def generate_extraction_summary(**context):
    """
    Generates a summary of the extraction process.
    Provides basic information about the DAG run and processed years.
    """
    import json
    
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
        
        # Log summary for easy viewing in Airflow logs
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

# === DAG DEFINITION ===
with DAG(
    dag_id="idx_data_extraction_parallel_enhanced",
    default_args={
        "owner": "airflow",
        "retries": 2,  # Increased retries for robustness against transient issues
        "retry_delay": pendulum.duration(minutes=10),
        "email_on_failure": False,
        "email_on_retry": False,
        "depends_on_past": False,
    },
    description="Enhanced IDX Financial Data Extraction - Parallel processing by year with improved error handling",
    
    # --- SCHEDULER CONFIGURATION ---
    # Example: Run daily at 02:00 AM WIB (Asia/Jakarta timezone)
    # schedule="0 2 * * *", 
    # OR, for daily interval:
    # schedule=pendulum.duration(days=1),
    #
    # Keep 'None' for manual triggers only
    schedule=None,
    # -------------------------------
    
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"), # DAG will start considering runs from this date
    catchup=False, # Do not run for past missed schedules
    tags=["idx", "extraction", "parallel", "financial-data", "enhanced"],
    params={ # Define parameters that can be passed at DAG trigger
        "periods": Param(
            default=DEFAULT_PERIODS,
            type="string",
            title="Periode Laporan",
            description="Periode laporan yang akan diekstrak: tw1 (Q1), tw2 (Q2), tw3 (Q3), audit (Annual). Pisahkan dengan koma."
        ),
        "cleanup_old_data": Param(
            default=True,
            type="boolean",
            title="Cleanup Data Lama",
            description="Hapus file download yang lebih dari 7 hari"
        ),
    },
    max_active_runs=1, # Allow only one DAG run at a time to manage resources
    dagrun_timeout=pendulum.duration(hours=12), # Maximum time a DAG run can take
) as dag:

    # === SETUP TASKS ===
    
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
    
    # Conditional cleanup task (runs based on 'cleanup_old_data' parameter)
    cleanup_task = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data,
        provide_context=True,
    )
    
    # BashOperator to check system resources (useful for debugging resource issues)
    check_resources = BashOperator(
        task_id="check_system_resources",
        bash_command="""
        echo "=== SYSTEM RESOURCES CHECK ==="
        echo "Memory Usage:"
        free -h
        echo ""
        echo "Disk Usage:"
        df -h /opt/airflow/data || df -h /tmp # Check disk space in data dir or fallback to /tmp
        echo ""
        echo "CPU Info (number of processors):"
        nproc
        echo ""
        echo "=== CHECK COMPLETED ==="
        """,
    )
    
    start_extractions = EmptyOperator(task_id="start_all_extractions")
    
    # === EXTRACTION TASK GROUP ===
    # This TaskGroup allows for parallel execution of extraction tasks for each year
    with TaskGroup("extraction_by_year") as extraction_group:
        extraction_tasks = []
        
        for year in YEARS_TO_SCRAPE:
            extraction_task = PythonOperator(
                task_id=f"extract_year_{year}",
                python_callable=import_and_run_extraction,
                op_kwargs={
                    "year": year,
                    "periods": "{{ params.periods }}" 
                },
                pool=EXTRACTION_POOL_NAME, # Tasks in this pool will respect its slot limit
                pool_slots=1, # Each task consumes 1 slot from the pool
                execution_timeout=pendulum.duration(hours=6), # Max time for a single extraction task
                retries=1, # Task-specific retry (can be different from DAG's default)
                retry_delay=pendulum.duration(minutes=15),
                provide_context=True,
            )
            
            extraction_tasks.append(extraction_task)
    
    # === FINAL TASKS ===
    
    end_extractions = EmptyOperator(task_id="end_all_extractions")
    
    generate_summary = PythonOperator(
        task_id="generate_extraction_summary",
        python_callable=generate_extraction_summary,
        provide_context=True,
        trigger_rule="none_failed_min_one_success", # This task will run even if some extraction tasks failed, as long as at least one succeeded.
    )
    
    # Final system-level cleanup (e.g., killing lingering browser processes)
    final_cleanup = BashOperator(
        task_id="final_system_cleanup",
        bash_command="""
        echo "=== FINAL SYSTEM CLEANUP ==="
        pkill -f firefox || true
        pkill -f chrome || true
        pkill -f geckodriver || true 
        pkill -f chromedriver || true 
        pkill -f selenium || true
        
        find /tmp -name "*selenium*" -type f -mtime +1 -delete 2>/dev/null || true
        find /tmp -name "*firefox*" -type f -mtime +1 -delete 2>/dev/null || true
        find /tmp -name "rust_mozprofile*" -type d -mtime +1 -exec rm -rf {} + 2>/dev/null || true 
        
        echo "Final system cleanup completed"
        """,
        trigger_rule="all_done", # This task will always run regardless of upstream task success/failure.
    )
    
    # === TASK DEPENDENCIES ===
    
    # Setup phase
    validate_params >> setup_env >> check_resources
    
    # Conditional cleanup (runs after resource check)
    check_resources >> cleanup_task >> start_extractions
    
    # Main extraction flow (all year tasks run in parallel)
    start_extractions >> extraction_group >> end_extractions
    
    # Final phase
    end_extractions >> generate_summary >> final_cleanup

# === DAG DOCUMENTATION ===
dag.doc_md = """
# IDX Financial Data Extraction DAG - Enhanced Version

## Overview
This DAG extracts financial data from the IDX (Indonesia Stock Exchange) website in parallel by year.
It's an enhanced version with improved error handling, monitoring, and cleanup capabilities.

## Features
- ✅ **Parallel processing by year** using Airflow pools for efficient resource utilization.
- ✅ **Enhanced error handling and retry logic** at both DAG and individual task levels.
- ✅ **Automatic environment setup** including base and year-specific download directories, with write permission validation.
- ✅ **System resource monitoring** via a Bash task to aid in debugging performance issues.
- ✅ **Automatic cleanup** of old temporary download files (older than 7 days) and empty company directories. **(Note: Extracted company data is removed immediately after being saved to MongoDB)**
- ✅ **Comprehensive logging and summary generation** for better operational visibility.
- ✅ **Flexible parameter configuration** allowing users to select specific periods and toggle cleanup.
- ✅ **Year-specific download folders** (`/opt/airflow/data/year_YYYY`) to organize extracted files.
- ✅ **Year and Period-specific MongoDB collections** (`reports_YYYY_period`) for structured data storage, ensuring data segregation.

## Parameters
- **periods**: A comma-separated string of report periods to extract (e.g., `tw1,tw2,tw3,audit`).
- **cleanup_old_data**: A boolean (True/False) to enable or disable the cleanup of old downloaded files.

## Years Processed
The current configuration processes data for the years: 2021, 2022, 2023, 2024.

## Requirements
- **MongoDB connection:** The Airflow worker needs access to a MongoDB instance. Ensure the `MONGO_URI` environment variable is set for your Airflow worker, e.g., `MONGO_URI=mongodb://your_mongodb_host:27017/`.
- **Firefox browser and geckodriver:** The Airflow worker environment must have Firefox and its WebDriver (geckodriver) installed and accessible in the system's PATH.
- **Sufficient disk space:** Crucially, ensure ample free disk space in `/opt/airflow/data` (for downloads) and `/tmp` (for browser temporary files).
- **Airflow Pool 'idx_extraction_pool' configured:** This pool must be created in your Airflow UI (Admin -> Pools) with an appropriate number of `slots`. This controls the degree of parallelism for your Selenium-based extraction tasks. A good starting point is `5` to `10` slots, depending on your worker's resources.

## Monitoring
Check the Airflow task logs for detailed progress and any issues during extraction. A summary will be generated at the end of the DAG run, providing overall statistics.
"""