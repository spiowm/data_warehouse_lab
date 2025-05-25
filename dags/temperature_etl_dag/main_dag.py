# dags/temperature_etl_dag/main_dag.py
import pendulum
import datetime
from airflow import DAG
from airflow.sdk import Variable

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator

from temperature_etl_dag.datasets_definition import ALL_TEMPERATURE_DATASETS
from temperature_etl_dag.processing import transform_and_save_data

default_args = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
}

with DAG(
        dag_id="temperature_data_etl_strict_schema",
        default_args=default_args,
        description="ETL для температурних даних згідно з визначеною схемою.",
        schedule="@month",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        tags={'temperatures', 'etl', 'lab2_updated'},
        doc_md="""
    ###  обробка даних про температуру

    Цей DAG виконує наступні кроки:
    1. Очікує на наявність вхідного CSV файлу.
    2. Трансформує дані згідно з чітко визначеною схемою (DimDate, DimCity, FactMonthlyTemperatures).
    3. Зберігає трансформовані дані в JSON файли, оновлюючи відповідні Airflow Datasets.
    """
) as dag:
    wait_for_raw_data_file = FileSensor(
        task_id='wait_for_raw_data_file',
        poke_interval=30,
        timeout=300,
        mode='poke',
        filepath=str(Variable.get('raw_temperature_input_filename')), # Забезпечуємо, що це рядок
        fs_conn_id='temperature_data_folder_conn'
    )

    process_and_save_datasets = PythonOperator(
        task_id='process_and_save_datasets',
        python_callable=transform_and_save_data,
        outlets=ALL_TEMPERATURE_DATASETS
    )

    wait_for_raw_data_file >> process_and_save_datasets