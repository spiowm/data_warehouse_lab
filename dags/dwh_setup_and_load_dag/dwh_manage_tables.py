# airflow_project/dags/dwh_setup_and_load_dag/dwh_manage_tables.py
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_dwh_tables(postgres_conn_id: str, relative_sql_file_path: str):
    """
    Виконує SQL-скрипт для створення таблиць у DWH.

    :param postgres_conn_id: ID з'єднання Airflow до PostgreSQL.
    :param relative_sql_file_path: Відносний шлях до .sql файлу
                                   (наприклад, 'sql/create_tables.sql')
                                   відносно директорії, де знаходиться цей .py файл.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Визначаємо абсолютний шлях до SQL файлу
    # __file__ - це шлях до поточного виконуваного файлу (dwh_manage_tables.py)
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_sql_file_path = os.path.join(current_script_dir, relative_sql_file_path)

    try:
        with open(absolute_sql_file_path, 'r', encoding='utf-8') as f:
            sql_statements = f.read()

        print(f"Виконання SQL-скрипту з файлу: {absolute_sql_file_path}")
        # PostgresHook.run може виконувати рядок, що містить декілька SQL-команд,
        # розділених крапкою з комою, якщо СУБД це підтримує (PostgreSQL підтримує).
        pg_hook.run(sql=sql_statements, autocommit=True)
        print(f"Таблиці успішно створені за допомогою {absolute_sql_file_path}")

    except FileNotFoundError:
        print(f"ПОМИЛКА: SQL файл не знайдено за шляхом: {absolute_sql_file_path}")
        raise
    except Exception as e:
        print(f"Помилка під час створення таблиць: {e}")
        raise