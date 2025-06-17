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
EXTRACTION_POOL_NAME = "idx_etl_pool"
BASE_DOWNLOAD_DIR = "/opt/airflow/data"
DEFAULT_PERIODS = "tw1,tw2,tw3,audit"

TRANSFORM_LOAD_POOL_NAME = EXTRACTION_POOL_NAME
MONGODB_READ_POOL_NAME = EXTRACTION_POOL_NAME

def validate_parameters(**context):
    try:
        periods = DEFAULT_PERIODS
        years = YEARS_TO_SCRAPE
        valid_periods = ['tw1', 'tw2', 'tw3', 'audit']
        input_periods = [p.strip().lower() for p in periods.split(',')]

        for period in input_periods:
            if period not in valid_periods:
                raise ValueError(f"Invalid period: {period}. Valid periods: {valid_periods}")

        for year in years:
            year_int = int(year)
            if year_int < 2020 or year_int > 2030:
                raise ValueError(f"Year {year} is out of range")

        logger.info("Parameters validated")
        return {'status': 'validated', 'years': years, 'periods': periods, 'validated_periods': input_periods}

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
    periods = DEFAULT_PERIODS

    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_path = os.path.join(current_script_dir, 'idx', 'scripts')

        if scripts_path not in sys.path:
            sys.path.insert(0, scripts_path)

        from idx.idx_extraction_script import main_extraction_task
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb-external:27017/")
        return main_extraction_task(year=year, periods=periods, mongo_uri=mongo_uri)

    except Exception as e:
        raise AirflowException(f"Extraction failed for year {year}: {e}")

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
    return {
        'dag_run_id': dag_run.run_id,
        'execution_date': dag_run.execution_date.isoformat(),
        'years_processed': YEARS_TO_SCRAPE,
        'total_years': len(YEARS_TO_SCRAPE),
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
    "retries": 0,
    "email_on_retry": False,
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
}

with DAG(
    dag_id="idx_data_ETL_pipeline_parallel",
    default_args=default_args,
    description="IDX Financial Data ETL - Parallel",
    schedule="0 0 1 */3 *",
    catchup=False,
    tags=["idx", "ETL", "extraction", "transform", "load"],
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=18),
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

    start_extractions = EmptyOperator(task_id="start_all_extractions")

    with TaskGroup("extraction_by_year") as extraction_group:
        extraction_tasks = []
        for year in YEARS_TO_SCRAPE:
            extraction_tasks.append(PythonOperator(
                task_id=f"extract_year_{year}",
                python_callable=import_and_run_extraction,
                op_kwargs={"year": year, "periods": DEFAULT_PERIODS},
                pool=EXTRACTION_POOL_NAME,
                pool_slots=2,
                execution_timeout=pendulum.duration(hours=6),
            ))

    end_extractions = EmptyOperator(task_id="end_all_extractions")

    generate_extraction_summary_task = PythonOperator(
        task_id="generate_extraction_summary",
        python_callable=generate_extraction_summary,
        trigger_rule="all_done",
    )

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
        pool=MONGODB_READ_POOL_NAME,
        pool_slots=1,
    )

    with TaskGroup("parallel_transform_load_collections") as transform_load_group:
        process_collection_tasks = PythonOperator.partial(
            task_id="process_single_collection",
            python_callable=process_and_load_single_collection,
            pool=TRANSFORM_LOAD_POOL_NAME,
            execution_timeout=pendulum.duration(minutes=60),
        ).expand(
            op_args=[get_collections_list.output]
        )

    end_transform_load = EmptyOperator(task_id="end_transform_load_process")

    # Dependencies
    validate_params >> setup_env >> cleanup_task >> check_resources
    check_resources >> start_extractions >> extraction_group >> end_extractions
    end_extractions >> generate_extraction_summary_task
    generate_extraction_summary_task >> wait_for_extraction_completion
    wait_for_extraction_completion >> start_transform_load
    start_transform_load >> check_spark_connection >> get_collections_list
    get_collections_list >> transform_load_group >> end_transform_load
