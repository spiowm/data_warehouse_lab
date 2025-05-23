# dags/temperature_etl_dag/datasets_definition.py
import os
from pathlib import Path
import json
from airflow import Dataset
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook

# Отримання базового шляху до папки даних з Connection
try:
    data_folder_conn = BaseHook.get_connection('temperature_data_folder_conn')
    DATA_BASE_PATH = str(json.loads(data_folder_conn.extra)['path'])
except Exception as e:
    # Запасний шлях, якщо Connection не знайдено під час парсингу
    print(f"Попередження: Не вдалося отримати 'temperature_data_folder_conn'. Використовуємо шлях за замовчуванням. Помилка: {e}")
    DATA_BASE_PATH = "/opt/airflow/dags/data"

# Отримання назви папки для оброблених датасетів з Variable
PROCESSED_FOLDER_NAME = str(Variable.get(
    "processed_datasets_output_folder", 
    default_var="processed_datasets"
))

# Конвертуємо в str для уникнення проблем з типами в os.path.join
PROCESSED_DATASETS_FULL_PATH = os.path.join(DATA_BASE_PATH, PROCESSED_FOLDER_NAME)
print(f"Базовий шлях для оброблених датасетів: {PROCESSED_DATASETS_FULL_PATH}")

# --- Визначення датасетів Airflow ---

# DimDate Dataset
# Використовуємо Path для кращого управління шляхами та конвертуємо в str
dim_date_path = str(Path(PROCESSED_DATASETS_FULL_PATH) / 'dim_date.json')
dim_date_uri = f"file://{dim_date_path}"
dim_date_dataset = Dataset(
    uri=dim_date_uri,
    extra={
        'description': 'Таблиця вимірів для Дат.',
        'columns': [ # Колонки з моєї Лаб1
            'DateSK', 'FullDate', 'Year', 'Month', 'MonthName',
            'DayOfMonth', 'Quarter', 'Season', 'Decade'
        ]
    }
)

# DimCity Dataset
dim_city_path = str(Path(PROCESSED_DATASETS_FULL_PATH) / 'dim_city.json')
dim_city_uri = f"file://{dim_city_path}"
dim_city_dataset = Dataset(
    uri=dim_city_uri,
    extra={
        'description': 'Таблиця вимірів для Міст.',
        'columns': [ # Колонки з моєї Лаб1
            'CitySK', 'CityName', 'CountryName', 'Latitude_str',
            'Longitude_str', 'Latitude_val', 'Longitude_val',
            'Continent', 'Hemisphere'
        ]
    }
)

# FactMonthlyTemperatures Dataset
fact_temperatures_path = str(Path(PROCESSED_DATASETS_FULL_PATH) / 'fact_monthly_temperatures.json')
fact_temperatures_uri = f"file://{fact_temperatures_path}"
fact_temperatures_dataset = Dataset(
    uri=fact_temperatures_uri,
    extra={
        'description': 'Таблиця фактів для місячних температур.',
        'columns': [ # Колонки з моєї Лаб1
            'DateSK_FK', 'CitySK_FK', 'AverageTemperature',
            'AverageTemperatureUncertainty'
        ]
    }
)

# Список всіх датасетів для зручного використання в інших модулях
ALL_TEMPERATURE_DATASETS = [dim_date_dataset, dim_city_dataset, fact_temperatures_dataset]
