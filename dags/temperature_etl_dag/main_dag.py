# dags/temperature_etl_dag/main_dag.py
import pendulum
import datetime
from airflow import DAG
from airflow.models.variable import Variable

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator

from temperature_etl_dag.processing import transform_and_save_data
from temperature_etl_dag.datasets_definition import ALL_TEMPERATURE_DATASETS

# Значення за замовчуванням для DAG
default_args = {
    'owner': 'airflow_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
}

with DAG(
        dag_id="temperature_data_etl_pipeline",
        default_args=default_args,
        description="ETL трубопровід для даних про глобальну температуру з CSV в JSON датасети.",
        schedule="@hourly",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        tags={'temperatures', 'etl', 'lab2_updated'},
        doc_md="""
    ### Трубопровід обробки даних про температуру

    Цей DAG виконує наступні кроки:
    1. **Очікує на наявність вхідного CSV файлу** у вказаному місці.
    2. **Трансформує дані:**
        - Читає дані про температуру з CSV.
        - Створює DimDate, DimCity та FactMonthlyTemperatures на основі моделі даних з Лаб1.
        - Вся текстова інформація в датасетах (наприклад, назви пір року) подається англійською.
    3. **Зберігає трансформовані дані** в окремі JSON файли, оновлюючи відповідні Airflow Datasets.
    """
) as dag:
    # Task 1: Сенсор для перевірки наявності вхідного файлу CSV
    # filepath тут відносний до шляху, вказаного в fs_conn_id
    wait_for_raw_data_file = FileSensor(
        task_id='wait_for_raw_data_file',
        poke_interval=30,  # Як часто перевіряти (в секундах)
        timeout=300,  # Максимальний час очікування (в секундах)
        mode='poke',  # Режим перевірки
        filepath=str(Variable.get('raw_temperature_input_filename')),  # Назва файлу з явним перетворенням на str
        fs_conn_id='temperature_data_folder_conn'  # Connection до папки з даними
    )

    # Task 2: Таск для трансформації даних та збереження JSON
    process_and_save_datasets = PythonOperator(
        task_id='process_and_save_datasets',
        python_callable=transform_and_save_data,
        # provide_context=True, - не потрібно в Airflow 2.0+, контекст передається автоматично
        outlets=ALL_TEMPERATURE_DATASETS  # Які датасети цей таск оновлює
    )

    # Визначення послідовності виконання тасків
    wait_for_raw_data_file >> process_and_save_datasets
