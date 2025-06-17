from __future__ import annotations

# ==== Standard Library ====
import os
import sys
import logging
import shutil

# Tambahkan direktori saat ini ke sys.path
scripts_path = os.path.dirname(os.path.abspath(__file__))
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

# ==== Third-party ====
import pendulum

# ==== Airflow ====
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException

# Setup logging
logger = logging.getLogger(__name__)

YEARS_TO_SCRAPE = ["2020", "2021", "2022", "2023", "2024", "2025"]
PERIODS_TO_SCRAPE = ["tw1", "tw2", "tw3", "audit"]
EXTRACTION_POOL_NAME = "idx_etl_pool"
BASE_DOWNLOAD_DIR = "/opt/airflow/data"

TRANSFORM_LOAD_POOL_NAME = EXTRACTION_POOL_NAME
MONGODB_READ_POOL_NAME = EXTRACTION_POOL_NAME

def validate_parameters(**context):
    try:
        years = YEARS_TO_SCRAPE
        periods = PERIODS_TO_SCRAPE
        
        for period in periods:
            if period not in ['tw1', 'tw2', 'tw3', 'audit']:
                raise ValueError(f"Invalid period: {period}. Valid periods: ['tw1', 'tw2', 'tw3', 'audit']")

        for year in years:
            year_int = int(year)
            if year_int < 2020 or year_int > 2030:
                raise ValueError(f"Year {year} is out of range")

        logger.info("Parameters validated")
        return {'status': 'validated', 'years': years, 'periods': periods}

    except Exception as e:
        raise AirflowException(f"Parameter validation failed: {e}")

def setup_environment(**context):
    try:
        os.makedirs(BASE_DOWNLOAD_DIR, exist_ok=True)
        test_file = os.path.join(BASE_DOWNLOAD_DIR, "test.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info("Environment setup complete")
        return {'status': 'environment_ready', 'base_dir': BASE_DOWNLOAD_DIR}
    except Exception as e:
        raise AirflowException(f"Environment setup failed: {e}")

def import_and_run_extraction(**context):
    year = context.get('year')
    period = context.get('period')

    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')

        if scripts_path not in sys.path:
            sys.path.insert(0, scripts_path)

        from idx.idx_extraction_script import main_extraction_task
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb-external:27017/")
        
        logger.info(f"Starting extraction for year {year}, period {period}")
        result = main_extraction_task(year=year, periods=period, mongo_uri=mongo_uri)
        logger.info(f"Completed extraction for year {year}, period {period}")
        return result

    except Exception as e:
        raise AirflowException(f"Extraction failed for year {year}, period {period}: {e}")

def cleanup_year_directories(**context):
    cleanup_count = 0
    for year in YEARS_TO_SCRAPE:
        year_dir_path = os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}")
        if os.path.exists(year_dir_path):
            shutil.rmtree(year_dir_path)
            cleanup_count += 1

    for item in os.listdir(BASE_DOWNLOAD_DIR):
        item_path = os.path.join(BASE_DOWNLOAD_DIR, item)
        if os.path.isfile(item_path) and item.lower().endswith((".zip", ".part", ".tmp", ".png", ".log")):
            os.remove(item_path)
            cleanup_count += 1

    return {'status': 'cleanup_completed', 'items_removed': cleanup_count}

def generate_extraction_summary(**context):
    dag_run = context['dag_run']
    total_combinations = len(YEARS_TO_SCRAPE) * len(PERIODS_TO_SCRAPE)
    return {
        'dag_run_id': dag_run.run_id,
        'execution_date': dag_run.execution_date.isoformat(),
        'years_processed': YEARS_TO_SCRAPE,
        'periods_processed': PERIODS_TO_SCRAPE,
        'total_years': len(YEARS_TO_SCRAPE),
        'total_periods': len(PERIODS_TO_SCRAPE),
        'total_combinations': total_combinations,
        'status': 'completed',
        'base_download_dir': BASE_DOWNLOAD_DIR
    }

# === TRANSFORM FUNCTIONS ===
try:
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)

    from idx.idx_transformation_load_script import (
        get_source_collections,
        process_and_load_single_collection,
        create_spark_session
    )
except ImportError as e:
    raise AirflowException(f"Import transform functions failed: {e}")

# === DAG DEFINITION ===
default_args = {
    "owner": "airflow",
    "retry_delay": pendulum.duration(minutes=10),
    "email_on_failure": False,
    "retries": 1,
    "email_on_retry": False,
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
}

with DAG(
    dag_id="idx_data_ETL_pipeline_sequential",
    default_args=default_args,
    description="IDX Financial Data ETL - Sequential Processing",
    schedule="0 0 1 */3 *",
    catchup=False,
    tags=["idx", "ETL", "extraction", "transform", "load", "sequential"],
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=24),
) as dag:

    validate_params = PythonOperator(
        task_id="validate_parameters",
        python_callable=validate_parameters,
    )

    setup_env = PythonOperator(
        task_id="setup_environment",
        python_callable=setup_environment,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_year_data_folders",
        python_callable=cleanup_year_directories,
    )

    check_resources = BashOperator(
        task_id="check_system_resources",
        bash_command="free -h && df -h /opt/airflow/data || df -h /tmp && nproc",
    )

    start_extractions = EmptyOperator(task_id="start_sequential_extractions")

    # Sequential extraction tasks - year by year, period by period
    extraction_tasks = []
    previous_task = start_extractions
    
    for year in YEARS_TO_SCRAPE:
        for period in PERIODS_TO_SCRAPE:
            task_id = f"extract_year_{year}_period_{period}"
            
            extraction_task = PythonOperator(
                task_id=task_id,
                python_callable=import_and_run_extraction,
                op_kwargs={"year": year, "period": period},
                pool=EXTRACTION_POOL_NAME,
                pool_slots=1,  # Sequential processing - only 1 slot
                execution_timeout=pendulum.duration(hours=2),
            )
            
            # Set dependency to previous task for sequential execution
            previous_task >> extraction_task
            extraction_tasks.append(extraction_task)
            previous_task = extraction_task

    end_extractions = EmptyOperator(task_id="end_sequential_extractions")

    generate_extraction_summary_task = PythonOperator(
        task_id="generate_extraction_summary",
        python_callable=generate_extraction_summary,
        trigger_rule="all_done",
    )

    start_transform_load = EmptyOperator(task_id="start_transform_load_process")
    wait_for_extraction_completion = EmptyOperator(task_id="wait_for_extraction_completion")

    check_spark_connection = PythonOperator(
        task_id="check_spark_session_pre_transform",
        python_callable=lambda **context: create_spark_session().stop(),
        execution_timeout=pendulum.duration(minutes=5),
        pool=TRANSFORM_LOAD_POOL_NAME,
        pool_slots=1,
        retries=2,  # Add retries for Spark connection check
    )

    get_collections_list = PythonOperator(
        task_id="get_source_collections_from_mongodb",
        python_callable=get_source_collections,
        pool=MONGODB_READ_POOL_NAME,
        pool_slots=1,
    )

    # Sequential transform and load - create individual tasks for each collection
    def get_collections_and_create_tasks(**context):
        """Get collections list and return it for sequential processing"""
        collections = get_source_collections(**context)
        logger.info(f"Found {len(collections)} collections to process: {collections}")
        return collections

    get_collections_task = PythonOperator(
        task_id="get_collections_for_sequential_processing",
        python_callable=get_collections_and_create_tasks,
        pool=MONGODB_READ_POOL_NAME,
        pool_slots=1,
    )


    process_collections_sequential = PythonOperator(
        task_id="process_all_collections_sequentially",
        python_callable= process_and_load_single_collection,
        pool=TRANSFORM_LOAD_POOL_NAME,
        pool_slots=1,
        execution_timeout=pendulum.duration(hours=6),  # Increased timeout for sequential processing
        retries=1,  # Add retry for transform process
        retry_delay=pendulum.duration(minutes=15),  # Longer retry delay for transform
    )

    def generate_final_summary(**context):
        """Generate comprehensive summary of the entire ETL process"""
        try:
            dag_run = context['dag_run']
            ti = context['ti']
            
            # Get extraction summary
            extraction_summary = ti.xcom_pull(task_ids='generate_extraction_summary')
            
            # Get transform summary
            transform_summary = ti.xcom_pull(task_ids='process_all_collections_sequentially')
            
            total_combinations = len(YEARS_TO_SCRAPE) * len(PERIODS_TO_SCRAPE)
            
            final_summary = {
                'dag_run_id': dag_run.run_id,
                'execution_date': dag_run.execution_date.isoformat(),
                'total_extraction_combinations': total_combinations,
                'years_processed': YEARS_TO_SCRAPE,
                'periods_processed': PERIODS_TO_SCRAPE,
                'extraction_summary': extraction_summary,
                'transform_summary': transform_summary,
                'pipeline_status': 'completed',
                'total_runtime_hours': (pendulum.now() - dag_run.execution_date).total_seconds() / 3600
            }
            
            logger.info(f"=== ETL PIPELINE SUMMARY ===")
            logger.info(f"DAG Run ID: {final_summary['dag_run_id']}")
            logger.info(f"Execution Date: {final_summary['execution_date']}")
            logger.info(f"Total Extraction Tasks: {total_combinations}")
            logger.info(f"Transform Collections Processed: {transform_summary.get('processed_count', 0) if transform_summary else 0}")
            logger.info(f"Transform Collections Failed: {transform_summary.get('failed_count', 0) if transform_summary else 0}")
            logger.info(f"Total Runtime: {final_summary['total_runtime_hours']:.2f} hours")
            logger.info(f"Pipeline Status: {final_summary['pipeline_status']}")
            
            return final_summary
            
        except Exception as e:
            logger.error(f"Failed to generate final summary: {e}")
            return {'status': 'summary_failed', 'error': str(e)}

    generate_final_summary_task = PythonOperator(
        task_id="generate_final_pipeline_summary",
        python_callable=generate_final_summary,
        trigger_rule="all_done",  # Run even if some tasks failed
    )

    end_transform_load = EmptyOperator(task_id="end_transform_load_process")

    # Dependencies
    validate_params >> setup_env >> cleanup_task >> check_resources
    check_resources >> start_extractions
    
    # Connect the last extraction task to end_extractions
    if extraction_tasks:
        extraction_tasks[-1] >> end_extractions
    else:
        start_extractions >> end_extractions
    
    end_extractions >> generate_extraction_summary_task
    generate_extraction_summary_task >> wait_for_extraction_completion
    wait_for_extraction_completion >> start_transform_load
    start_transform_load >> check_spark_connection >> get_collections_task
    get_collections_task >> process_collections_sequential >> end_transform_load
    end_transform_load >> generate_final_summary_task