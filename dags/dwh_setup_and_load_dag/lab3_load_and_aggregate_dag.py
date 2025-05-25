# airflow_project/dags/dwh_setup_and_load_dag/lab3_load_and_aggregate_dag.py

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dwh_setup_and_load_dag.dwh_manage_tables import create_dwh_tables
from dwh_setup_and_load_dag.dwh_load_data import (
    load_dim_date_from_json,
    load_dim_city_from_json,
    load_fact_monthly_temperatures_from_json
)

from temperature_etl_dag.datasets_definition import (
    dim_date_dataset,
    dim_city_dataset,
    fact_temperatures_dataset
)

# ID з'єднання Airflow до PostgreSQL
POSTGRES_CONN_ID = "postgres_dwh_conn"  # Актуальний Connection ID

with DAG(
        dag_id="lab3_dwh_load_and_aggregate_v2",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule=[dim_date_dataset, dim_city_dataset, fact_temperatures_dataset],
        catchup=False,
        tags={'dwh', 'temperatures', 'lab3', 'load', 'aggregate', 'common-sql'},
        doc_md="""
    ### Лабораторна робота №3: Завантаження та Агрегація DWH (з SQLExecuteQueryOperator)

    Цей DAG виконує наступні кроки:
    1. Створює (якщо не існують) таблиці DWH.
    2. Завантажує дані з JSON файлів у таблиці DimDate, DimCity.
    3. Завантажує дані з JSON файлу у таблицю FactMonthlyTemperatures.
    4. Виконує SQL агрегацію.

    Використовує SQLExecuteQueryOperator замість застарілого PostgresOperator.
    """
) as dag:
    create_tables_task = PythonOperator(
        task_id="create_dwh_tables",
        python_callable=create_dwh_tables,
        op_kwargs={
            "postgres_conn_id": POSTGRES_CONN_ID,
            "relative_sql_file_path": "sql/create_tables.sql"
        },
    )

    load_dim_date_task = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date_from_json,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )

    load_dim_city_task = PythonOperator(
        task_id="load_dim_city",
        python_callable=load_dim_city_from_json,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )

    load_fact_temperatures_task = PythonOperator(
        task_id="load_fact_monthly_temperatures",
        python_callable=load_fact_monthly_temperatures_from_json,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )

    aggregate_data_task = SQLExecuteQueryOperator(
        task_id="aggregate_yearly_city_temperature",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/aggregate_data.sql",
    )

    create_tables_task >> [load_dim_date_task, load_dim_city_task]
    [load_dim_date_task, load_dim_city_task] >> load_fact_temperatures_task
    load_fact_temperatures_task >> aggregate_data_task
