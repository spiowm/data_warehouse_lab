# airflow_project/dags/dwh_setup_and_load_dag/dwh_load_data.py
import json
import os
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_processed_datasets_path() -> str:
    """
    Повертає абсолютний шлях до папки 'data/processed_datasets/'
    відносно кореневої папки DAGs в Airflow.
    Припускає стандартну структуру: AIRFLOW_HOME/dags/data/processed_datasets/
    """
    airflow_home_path = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    dags_folder_path = os.path.join(airflow_home_path, 'dags')
    target_path = Path(dags_folder_path) / "data" / "processed_datasets"

    if not target_path.is_dir():
        print(f"ПОПЕРЕДЖЕННЯ: Директорія датасетів не знайдена: {target_path}. "
              f"Перевірте структуру папок та змінну AIRFLOW_HOME (поточна: {airflow_home_path}).")
    return str(target_path)


def load_json_to_table(postgres_conn_id: str,
                       table_name_snake_case: str,
                       json_filename: str,
                       db_columns_snake_case: list,
                       pkey_db_columns_snake_case: list = None,
                       truncate_before_load: bool = True):
    """
    Завантажує дані з JSON файлу в таблицю PostgreSQL.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    processed_datasets_dir = get_processed_datasets_path()
    json_file_path = Path(processed_datasets_dir) / json_filename

    if not json_file_path.is_file():
        print(f"ПОМИЛКА: JSON файл не знайдено: {json_file_path}. "
              f"Завантаження для таблиці {table_name_snake_case} пропущено.")
        return

    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data_from_json = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ПОМИЛКА: Не вдалося розпарсити JSON файл {json_file_path}: {e}")
        raise

    if not data_from_json:
        print(f"Файл {json_filename} порожній. Завантаження для {table_name_snake_case} пропущено.")
        return

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        if truncate_before_load:
            print(f"Очищення таблиці {table_name_snake_case} (TRUNCATE ... CASCADE)...")
            # Назви таблиць в PostgreSQL без лапок автоматично приводяться до нижнього регістру
            cursor.execute(f'TRUNCATE TABLE {table_name_snake_case} CASCADE;')

        # Назви колонок БД вже в snake_case і відповідають ключам JSON
        cols_db_str = ", ".join(db_columns_snake_case)
        vals_placeholder = ", ".join(["%s"] * len(db_columns_snake_case))

        insert_query = f'INSERT INTO {table_name_snake_case} ({cols_db_str}) VALUES ({vals_placeholder})'

        if pkey_db_columns_snake_case:
            conflict_target = ", ".join(pkey_db_columns_snake_case)
            update_set_arr = [f"{col_db} = EXCLUDED.{col_db}"
                              for col_db in db_columns_snake_case if col_db not in pkey_db_columns_snake_case]
            if update_set_arr:
                update_set_str = ", ".join(update_set_arr)
                insert_query += f" ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set_str}"
            else:
                insert_query += f" ON CONFLICT ({conflict_target}) DO NOTHING"

        records_to_insert = []
        for record_json in data_from_json:
            # Ключі в JSON (record_json) тепер мають бути в snake_case
            # і відповідати db_columns_snake_case
            record_values = [record_json.get(db_col) for db_col in db_columns_snake_case]
            records_to_insert.append(tuple(record_values))

        if records_to_insert:
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            print(f"Успішно завантажено/оновлено {len(records_to_insert)} записів у таблицю {table_name_snake_case} з {json_filename}")
        else:
            print(f"Немає підготовлених даних для завантаження у {table_name_snake_case} з {json_filename}")

    except Exception as e:
        conn.rollback()
        print(f"Помилка під час завантаження даних у {table_name_snake_case}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# --- Специфічні функції-обгортки ---

def load_dim_date_from_json(postgres_conn_id: str):
    db_columns = [
        'date_sk', 'full_date', 'year', 'month', 'month_name',
        'year_month', 'quarter', 'season', 'decade'
    ]
    load_json_to_table(
        postgres_conn_id=postgres_conn_id,
        table_name_snake_case="dim_date",
        json_filename="dim_date.json",
        db_columns_snake_case=db_columns,
        pkey_db_columns_snake_case=["date_sk"],
        truncate_before_load=True
    )


def load_dim_city_from_json(postgres_conn_id: str):
    db_columns = [
        'city_sk', 'city_name', 'country_name',
        'latitude_val', 'longitude_val',
        'continent_name', 'hemisphere'
    ]
    load_json_to_table(
        postgres_conn_id=postgres_conn_id,
        table_name_snake_case="dim_city",
        json_filename="dim_city.json",
        db_columns_snake_case=db_columns,
        pkey_db_columns_snake_case=["city_sk"],
        truncate_before_load=True
    )


def load_fact_monthly_temperatures_from_json(postgres_conn_id: str):
    db_columns = [
        'date_sk',
        'city_sk',
        'average_temperature_celsius',
        'average_temperature_uncertainty'
    ]
    load_json_to_table(
        postgres_conn_id=postgres_conn_id,
        table_name_snake_case="fact_monthly_temperatures",
        json_filename="fact_monthly_temperatures.json",
        db_columns_snake_case=db_columns,
        pkey_db_columns_snake_case=["date_sk", "city_sk"],
        truncate_before_load=True
    )