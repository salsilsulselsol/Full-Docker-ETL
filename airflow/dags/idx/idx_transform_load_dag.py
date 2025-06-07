from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

# Impor fungsi dari skrip baru
from idx.scripts.idx_transformation_script import main_transformation_task

with DAG(
    dag_id="idx_data_transform_load",
    default_args={"owner": "airflow"},
    description="Transformasi dan load data finansial IDX dari staging ke production.",
    schedule=None, 
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=["idx", "transform", "load", "python"],
) as dag:
    start_transform_load = EmptyOperator(task_id="start_transform_load")

    # Sensor ini akan menunggu DAG ekstraksi paralel selesai
    # Pastikan external_dag_id cocok dengan dag_id DAG ekstraksi
    wait_for_extraction = ExternalTaskSensor(
        task_id="wait_for_idx_extraction_completion",
        external_dag_id="idx_data_extraction_parallel_by_year",
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="poke",
        poke_interval=60,
        timeout=7200,
    )

    run_transform_load_script = PythonOperator(
        task_id="run_idx_transform_load_script",
        python_callable=main_transformation_task,
    )

    end_transform_load = EmptyOperator(task_id="end_transform_load_process")

    start_transform_load >> wait_for_extraction >> run_transform_load_script >> end_transform_load